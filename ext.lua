nget = 0
nstore = 0

function engine_get(engine, cookie, key, vbucket)
  -- log(5, "lua ENGINE_GET for key: " .. key .. " vbucket: " .. vbucket)

  nget = nget + 1

  if key == "nget" then
     data = tostring(nget)
     rv, item = engine:allocate_item(cookie, key,
                                     string.len(data) + 2, 0, 0)
     if rv == 0 then
       rv = engine:set_item_data(cookie, item, 0, data)
       if rv == 0 then
         return 0, item
       end
     end

     return rv
  end

  if key == "nstore" then
     data = tostring(nstore)
     rv, item = engine:allocate_item(cookie, key,
                                     string.len(data) + 2, 0, 0)
     if rv == 0 then
       rv = engine:set_item_data(cookie, item, 0, data)
       if rv == 0 then
         return 0, item
       end
     end

     return rv
  end

  if key:sub(0, 7) == "prefix:" then
    rv, actual_item = engine:get(cookie, key, vbucket)
    if rv == 0 and actual_item then
      rv, actual_key, flags, exptime, cas, actual_nbytes, prefix =
        engine:get_item_data(cookie, actual_item, 0, 9)
      if rv == 0 and prefix then
        rv, item = engine:allocate_item(cookie, key,
                                        string.len(prefix) + 2,
                                        flags, exptime)
        if rv == 0 and item then
          rv = engine:set_item_data(cookie, item, 0, prefix)
          if rv == 0 then
            return 0, item
          end
        end
      end
    end

    return rv
  end

  return engine:get(cookie, key, vbucket)
end

function engine_store(engine, cookie, item, cas, operation, vbucket)
  -- log(5, "lua ENGINE_STORE vbucket: " .. vbucket)

  nstore = nstore + 1

  if operation == OPERATION_APPEND then
    rv, key, flags, exptime, old_cas, nbytes, data =
      engine:get_item_data(cookie, item, 0, -1)
    if rv == 0 and key and key:sub(0, 4) == "SET~" then
    end
  end

  return engine:store(cookie, item, cas, operation, vbucket)
end

function engine_flush_all(engine, cookie, when)
  -- log(5, "lua engine flush " .. engine:name(cookie))

  return engine:flush_all(cookie, when)
end

log(LOG_WARNING, "lua ext loaded")
