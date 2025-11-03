import Apify from 'apify';
import axios from 'axios';

const { Actor, log, sleep, KeyValueStore, Dataset } = Apify;

// ---------- Helpers ---------- 
const normalizeHandle = (h) => {
  if (!h) return null;
  const clean = String(h).trim().replace(/^@+/, '');
  return clean ? `@${clean}` : null;
};

const isUSProfile = (profile) => {
  const region = (profile.region || profile.country || '').toLowerCase();
  const bio = (profile.signature || profile.bio || '').toLowerCase();
  const location = (profile.location || '').toLowerCase();
  const name = (profile.nickname || profile.fullName || '').toLowerCase();
  const text = `${region} ${location} ${bio} ${name}`;

  if (/\b(united states|usa|u\.s\.a|u\.s\.|🇺🇸)\b/.test(text)) return true;
  if (/\b[A-Za-z ]+,\s?(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b/i.test(text)) return true;
  return false;
};

// Airtable mini client
const airtableClient = ({ apiKey, baseId, tableName }) => {
  const baseURL = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  const headers = { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' };
  const pause = async () => sleep(220); // ~4–5 rps

  const findByHandle = async (handleField, handleValue) => {
    const formula = `LOWER(${handleField})='${handleValue.toLowerCase()}'`;
    const url = `${baseURL}?maxRecords=1&filterByFormula=${encodeURIComponent(formula)}`;
    const res = await axios.get(url, { headers });
    await pause();
    return res.data?.records?.[0] || null;
  };

  const createRecord = async (fields) => {
    const res = await axios.post(baseURL, { records: [{ fields }] }, { headers });
    await pause();
    return res.data?.records?.[0] || null;
  };

  const updateRecord = async (id, fields) => {
    const res = await axios.patch(baseURL, { records: [{ id, fields }] }, { headers });
    await pause();
    return res.data?.records?.[0] || null;
  };

  return { findByHandle, createRecord, updateRecord };
};

const toUnifiedProfile = (raw) => {
  const usernameRaw =
    raw.username || raw.uniqueId || raw.handle || raw.user?.uniqueId || raw.author?.uniqueId;

  const handle = normalizeHandle(usernameRaw);
  const nickname = raw.nickname || raw.fullName || raw.user?.nickname || raw.author?.nickname || '';
  const followerCount =
    Number(raw.followers || raw.followerCount || raw.stats?.followerCount || raw.stats?.follower_count || 0) || 0;

  const bio = raw.signature || raw.bio || raw.user?.signature || raw.author?.signature || '';
  const url = raw.profileUrl || (handle ? `https://www.tiktok.com/${handle}` : raw.url || null);
  const email = raw.email || raw.businessEmail || null;
  const externalUrl = raw.link || raw.bioUrl || raw.externalUrl || null;
  const region = raw.region || raw.country || null;
  const topics = raw.topics || raw.hashtags || raw.tags || null;
  const language = raw.language || raw.lang || null;
  const location = raw.location || null;

  return {
    handle,
    nickname,
    followerCount,
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

// ---------- Main ----------
await Actor.main(async () => {
  const input = await Actor.getInput();

  const {
    searchTerms = ["amazon finds", "etsy finds", "beauty product review", "drop shipping", "coupon codes"],
    hashtags = ["amazonfinds", "makeupreview", "tiktokmademebuyit", "tiktokshop"],
    maxProfilesPerSeed = 200,

    minFollowers = 10000,
    maxFollowers = 100000,
    requireUS = true,

    kvStoreName = "tiktok-dedupe",
    kvStoreKey = "seenUsernames",

    airtableEnabled = true,
    airtableTable = "Creators",
    airtableHandleField = "Handle",
    airtableUpsert = true,

    // Will be read from env if not set in input:
    airtableApiKey = Actor.getEnv().AIRTABLE_API_KEY,
    airtableBaseId = Actor.getEnv().AIRTABLE_BASE_ID,

    // TikTok scraper actor (choose a working one in your account/store)
    tiktokActorId = "apify/actor-tiktok-scraper",
    tiktokActorInput = {
      includeFullUserObjects: true,
      searchMode: "users"
    },

    dryRun = false
  } = input || {};

  // Defensive: if Airtable creds missing, auto-disable Airtable
  let airtableActive = airtableEnabled;
  if (airtableEnabled && (!airtableApiKey || !airtableBaseId)) {
    airtableActive = false;
    log.warning("Airtable enabled but secrets missing; continuing without Airtable upsert.");
  }

  const kv = await KeyValueStore.open(kvStoreName);
  const seen = (await kv.getValue(kvStoreKey)) || {}; // { '@handle': true }

  const at = airtableActive ? airtableClient({
    apiKey: airtableApiKey, baseId: airtableBaseId, tableName: airtableTable
  }) : null;

  const pushUnique = async (profile) => {
    if (!profile.handle || seen[profile.handle]) return false;
    seen[profile.handle] = true;
    await Dataset.pushData(profile);
    return true;
  };

  const withinBand = (n) => n >= minFollowers && n <= maxFollowers;

  const seeds = [
    ...searchTerms.map(s => ({ type: 'term', value: s })),
    ...hashtags.map(h => ({ type: 'hashtag', value: h.replace(/^#/, '') }))
  ];

  log.info(`Seeds: ${seeds.length} | Follower band: ${minFollowers}-${maxFollowers} | US required: ${requireUS}`);

  let candidates = 0;
  let saved = 0;
  let created = 0;
  let updated = 0;

  for (const seed of seeds) {
    log.info(`Running TikTok actor for ${seed.type}: ${seed.value}`);

    const callInput = {
      searchTerms: seed.type === 'term' ? [seed.value] : undefined,
      hashtags: seed.type === 'hashtag' ? [seed.value] : undefined,
      maxItems: maxProfilesPerSeed,
      ...tiktokActorInput
    };

    let run;
    try {
      run = await Actor.call(tiktokActorId, callInput, { waitSecs: 1800 });
    } catch (e) {
      log.exception(e, `Failed calling ${tiktokActorId} for seed ${seed.value}`);
      continue;
    }

    const dsId = run?.defaultDatasetId || run?.output?.defaultDatasetId || null;
    if (!dsId) {
      log.warning("No dataset ID returned by TikTok actor. Skipping seed.", { seed });
      continue;
    }

    const ds = await Dataset.open(dsId);
    const { items = [] } = await ds.getData({ limit: maxProfilesPerSeed });

    if (!items.length) {
      log.warning(`No items for seed "${seed.value}".`);
      continue;
    }

    for (const it of items) {
      const u = toUnifiedProfile(it);
      if (!u.handle) continue;

      candidates += 1;
      if (!withinBand(u.followerCount)) continue;
      if (requireUS && !isUSProfile(u)) continue;
      if (seen[u.handle]) continue; // in-run dedupe

      // Against Airtable (optional) + Upsert
      if (airtableActive && airtableUpsert) {
        try {
          const existing = await at.findByHandle(airtableHandleField, u.handle);
          const fields = {
            Handle: u.handle,
            "Full Name": u.nickname || null,
            Followers: u.followerCount,
            Bio: u.bio || null,
            "Profile URL": u.url || null,
            Email: u.email || null,
            "External URL": u.externalUrl || null,
            Region: u.region || null,
            Location: u.location || null,
            Language: u.language || null,
            Topics: Array.isArray(u.topics) ? u.topics.join(', ') : (u.topics || null),
            "Last Synced (UTC)": new Date().toISOString()
          };

          if (!dryRun) {
            if (existing) {
              await at.updateRecord(existing.id, fields);
              updated += 1;
            } else {
              await at.createRecord(fields);
              created += 1;
            }
          }
        } catch (e) {
          log.exception(e, `Airtable upsert failed for ${u.handle}; continuing without upsert.`);
        }
      } else if (airtableActive && !airtableUpsert) {
        // If not upserting, still avoid dupes using Airtable
        try {
          const existing = await at.findByHandle(airtableHandleField, u.handle);
          if (existing) continue;
        } catch (e) {
          log.exception(e, "Airtable lookup failed; ignoring for this record.");
        }
      }

      const ok = await pushUnique(u);
      if (ok) saved += 1;
    }
  }

  await kv.setValue(kvStoreKey, seen);
  log.info(`Done. Candidates scanned: ${candidates}, Saved: ${saved}, Airtable created: ${created}, updated: ${updated}`);
});
