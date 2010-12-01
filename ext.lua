local nget = 0
local nstore = 0

function engine_get(engine, cookie, key, vbucket)
  if key == "nget" then
     data = tostring(nget)
     rv, item = engine:allocate(cookie,
                                key, string.len(data), 0, 0)
     if rv == 0 then
       rv = engine:set_item_data(cookie, item, data)
       if rv == 0 then
         return 0, item
       end
     end

     return 1
  end

  if key == "nstore" then
     data = tostring(nstore)
     rv, item = engine:allocate(cookie,
                                key, string.len(data), 0, 0)
     if rv == 0 then
       rv = engine:set_item_data(cookie, item, data)
       if rv == 0 then
         return 0, item
       end
     end

     return 1
  end

  nget = nget + 1

  -- log(5, "lua ENGINE_GET for key: " .. key .. " vbucket: " .. vbucket)
  return engine:get(cookie, key, vbucket)
end

function engine_store(engine, cookie, item, cas, operation, vbucket)
  nstore = nstore + 1

  -- log(5, "lua ENGINE_STORE, vbucket: " .. vbucket)
  return engine:store(cookie, item, cas, operation, vbucket)
end

log(5, "lua ENGINE ext.lua loaded")
