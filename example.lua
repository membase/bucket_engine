function bucket_get(engine, cookie, key, vbucket)
  log(5, "lua BUCKET_GET for key: " .. key .. " vbucket: " .. vbucket)
  return bucket_engine.bucket_get(engine, cookie, key, vbucket)
end

function bucket_store(engine, cookie, item, cas, operation, vbucket)
  log(5, "lua BUCKET_STORE, vbucket: " .. vbucket)
  return bucket_engine.bucket_store(engine, cookie, item, cas, operation, vbucket)
end

bucket_engine.log(5, "lua loaded BUCKET_ENGINE")
