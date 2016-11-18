

local cjson = require 'cjson'
local conf = require 'conf_server'
local redis = require "resty.redis"
local game = {}

function game.poll(data)
	local args = cjson.decode(data)
	if not (args and args.server_id) then
		return 50, 'invalid server_id'
	end

	if not args.server_id then
		return 51, 'server id lost'
	end

	-- Pub/sub redis
	local red = redis:new()
  redis:set_timeout(1000*5)
	local redis_conf = conf.redis.poll
	local ok, err = red:connect(redis_conf.host, redis_conf.port)
	if not ok then
		return 52, 'redis lost'
	end

  local chan = "channel:game:"..args.server_id
	local sub_rep, sub_err = red:subscribe(chan)
  if not sub_rep then
    return 53, 'failed to subscribe: ' .. sub_err
  end

	redis:set_keepalive(1000*60, 100)  
  while true do
	  local read_data, read_err = red:read_reply()
  	if not read_data then
  		if read_err ~= 'timeout' then
  			-- TODO log
  			break
  		end
  	else
			redis:unsubscribe(chan)
		  --ngx.log(ngx.INFO, 'poll ', args.server_id)
		  return 0, read_data
  	end
	end
end

return game

