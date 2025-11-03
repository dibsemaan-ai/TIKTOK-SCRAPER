import Apify from 'apify';
import axios from 'axios';

const { Actor, log, sleep, KeyValueStore, Dataset } = Apify;

// --- Simple helpers ---------------------------------------------------------

const normalizeHandle = (h) => {
  if (!h) return null;
  const clean = h.trim().replace(/^@+/, '');
  return clean ? `@${clean}` : null;
};

// Basic US detection from profile (best-effort; TikTok doesn't expose reliable country)
const isUSProfile = (profile) => {
  const region = profile.region || profile.country || '';
  const bio = (profile.signature || profile.bio || '').toLowerCase();
  const location = (profile.location || '').toLowerCase();
  const name = (profile.nickname || profile.fullName || '').toLowerCase();

  // Common markers
  const markers = [
    'united states', 'usa', 'u.s.', 'u.s.a', 'us 🇺🇸', '🇺🇸'
  ];

  // State abbreviations (common ones; add more as needed)
  const states = ['ny', 'ca', 'tx', 'fl', 'il', 'pa', 'oh', 'mi', 'ga', 'nc', 'nj', 'va', 'wa', 'az', 'ma', 'tn', 'in', 'mo', 'md', 'wi', 'co', 'mn', 'sc', 'al', 'la', 'ky', 'or', 'ok', 'ct', 'ut', 'ia', 'ms', 'ar', 'nv', 'nm', 'ne', 'wv', 'id', 'hi', 'nh', 'me', 'mt', 'ri', 'de', 'sd', 'nd', 'ak', 'vt', 'dc'];

  const textBlob = `${region} ${location} ${bio} ${name}`.toLowerCase();

  if (/^us$/i.test(region) || /^USA$/i.test(region)) return true;
  if (markers.some(m => textBlob.includes(m))) return true;

  // crude: "city, ST" patterns
  if (/\b[A-Za-z ]+,\s?(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b/.test(textBlob)) return true;

  // fallback: explicit state abbrev alone
  if (states.some(st => new RegExp(`\\b${st}\\b`).test(textBlob))) return true;

  return false;
};

// Airtable mini client
const airtable = ({ apiKey, baseId, tableName }) => {
  const baseURL = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  const headers = { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' };

  const rateLimitPauseMs = 220; // ~4.5 rps safety

  const findByHandle = async (handleField, handleValue) => {
    const formula = `LOWER(${handleField})='${handleValue.toLowerCase()}'`;
    const url = `${baseURL}?maxRecords=1&filterByFormula=${encodeURIComponent(formula)}`;
    const res = await axios.get(url, { headers });
    await sleep(rateLimitPauseMs);
    return res.data.records?.[0] || null;
  };

  const createRecord = async (fields) => {
    const res = await axios.post(baseURL, { records: [{ fields }] }, { headers });
    await sleep(rateLimitPauseMs);
    return res.data.records?.[0];
  };

  const updateRecord = async (id, fields) => {
    const res = await axios.patch(baseURL, { records: [{ id, fields }] }, { headers });
    await sleep(rateLimitPauseMs);
    return res.data.records?.[0];
  };

  return { findByHandle, createRecord, updateRecord };
};

// Map various TikTok actor outputs to a single shape
const toUnifiedProfile = (raw) => {
  // Try multiple likely properties returned by different TikTok scrapers
  const usernameRaw =
    raw.username || raw.uniqueId || raw.handle || raw.user?.uniqueId || raw.author?.uniqueId;

  const handle = normalizeHandle(usernameRaw);
  const nickname = raw.nickname || raw.fullName || raw.user?.nickname || raw.author?.nickname || '';
  const followerCount =
    raw.followers || raw.followerCount || raw.stats?.followerCount || raw.stats?.follower_count || 0;

  const bio = raw.signature || raw.bio || raw.user?.signature || raw.author?.signature || '';
  const url = raw.profileUrl
    || (handle ? `https://www.tiktok.com/${handle}` : null)
    || raw.url;

  const email = raw.email || raw.businessEmail || null;
  const externalUrl = raw.link || raw.bioUrl || raw.externalUrl || null;
  const region = raw.region || raw.country || null;
  const topics = raw.topics || raw.hashtags || raw.tags || null;
  const language = raw.language || raw.lang || null;
  const location = raw.location || null;

  return {
    handle,
    nickname,
    followerCount: Number(followerCount) || 0,
    bio,
    url,
    email,
    externalUrl,
    region,
    topics,
    language,
    location,
    raw
  };
};

await Actor.main(async () => {
  const input = await Actor.getInput();

  const {
    // Search seed
    searchTerms = ["amazon finds", "etsy finds", "makeup review", "drop shipping", "coupon codes", "amazon must haves"],
    hashtags = ["amazonfinds", "makeupreview", "tiktokmademebuyit"],
    maxProfilesPerSeed = 200,

    // Filters
    minFollowers = 10000,
    maxFollowers = 100000,
    requireUS = true,

    // Deduping
    kvStoreName = "tiktok-dedupe",
    kvStoreKey = "seenUsernames",

    // Airtable
    airtableEnabled = true,
    airtableApiKey = Actor.getEnv().AIRTABLE_API_KEY,   // set in Actor secrets
    airtableBaseId = Actor.getEnv().AIRTABLE_BASE_ID,   // set in Actor secrets
    airtableTable = "Creators",
    airtableHandleField = "Handle",                     // unique key in Airtable
    airtableUpsert = true,

    // Which TikTok scraper actor to call
    // Use official/community actor id you prefer. This default works for popular community scrapers.
    tiktokActorId = "apify/actor-tiktok-scraper",
    tiktokActorInput = {},

    // Debug
    dryRun = false
  } = input || {};

  if (airtableEnabled && (!airtableApiKey || !airtableBaseId)) {
    throw new Error("Airtable is enabled but AIRTABLE_API_KEY or AIRTABLE_BASE_ID is not set.");
  }

  const kv = await KeyValueStore.open(kvStoreName);
  const alreadySeen = (await kv.getValue(kvStoreKey)) || {}; // { '@handle': true }

  const at = airtableEnabled
    ? airtable({ apiKey: airtableApiKey, baseId: airtableBaseId, tableName: airtableTable })
    : null;

  const pushUnique = async (profile) => {
    if (!profile.handle) return false;
    if (alreadySeen[profile.handle]) return false;
    alreadySeen[profile.handle] = true;
    await Dataset.pushData(profile);
    return true;
  };

  const withinRange = (f) => f >= minFollowers && f <= maxFollowers;

  const seeds = [
    ...searchTerms.map(s => ({ type: 'term', value: s })),
    ...hashtags.map(h => ({ type: 'hashtag', value: h.replace(/^#/, '') }))
  ];

  log.info(`Starting TikTok scrape over ${seeds.length} seeds.`);

  let totalCandidates = 0;
  let totalSaved = 0;
  let totalAirtableCreated = 0;
  let totalAirtableUpdated = 0;

  for (const seed of seeds) {
    log.info(`Seed: ${seed.type} → ${seed.value}`);

    // Call chosen TikTok scraper actor with a generic input that most support:
    const callInput = {
      searchTerms: seed.type === 'term' ? [seed.value] : undefined,
      hashtags: seed.type === 'hashtag' ? [seed.value] : undefined,
      maxItems: maxProfilesPerSeed,
      // Merge any custom user-provided input overrides:
      ...tiktokActorInput
    };

    const run = await Actor.call(tiktokActorId, callInput, { timeoutSecs: 1800 });
    const { defaultDatasetId } = run?.defaultDatasetId
      ? run
      : run?.output || {};

    const ds = await Dataset.open(defaultDatasetId || run.defaultDatasetId || 'default');
    const { items } = await ds.getData({ limit: maxProfilesPerSeed });
    if (!items?.length) {
      log.warning(`No items for seed "${seed.value}".`);
      continue;
    }

    for (const it of items) {
      const unified = toUnifiedProfile(it);
      if (!unified.handle) continue;

      totalCandidates += 1;

      // Range filter
      if (!withinRange(unified.followerCount)) continue;

      // US filter
      if (requireUS && !isUSProfile(unified)) continue;

      // De-dupe in-run
      if (alreadySeen[unified.handle]) continue;

      // De-dupe against Airtable & upsert
      if (airtableEnabled && airtableUpsert) {
        const existing = await at.findByHandle(airtableHandleField, unified.handle);
        const fields = {
          Handle: unified.handle,
          "Full Name": unified.nickname || null,
          Followers: unified.followerCount,
          Bio: unified.bio || null,
          "Profile URL": unified.url || null,
          Email: unified.email || null,
          "External URL": unified.externalUrl || null,
          Region: unified.region || null,
          Location: unified.location || null,
          Language: unified.language || null,
          Topics: Array.isArray(unified.topics) ? unified.topics.join(', ') : (unified.topics || null),
          "Last Synced (UTC)": new Date().toISOString()
        };

        if (!dryRun) {
          if (existing) {
            await at.updateRecord(existing.id, fields);
            totalAirtableUpdated += 1;
          } else {
            await at.createRecord(fields);
            totalAirtableCreated += 1;
          }
        }
      } else {
        // If not upserting, still avoid duplicates using Airtable (optional check)
        if (airtableEnabled) {
          const existing = await at.findByHandle(airtableHandleField, unified.handle);
          if (existing) continue;
        }
      }

      const saved = await pushUnique(unified);
      if (saved) totalSaved += 1;
    }
  }

  await kv.setValue(kvStoreKey, alreadySeen);

  log.info(`Done. Candidates scanned: ${totalCandidates}`);
  log.info(`Saved (unique, filtered): ${totalSaved}`);
  if (airtableEnabled) {
    log.info(`Airtable created: ${totalAirtableCreated}, updated: ${totalAirtableUpdated}`);
  }
});
