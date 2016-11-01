-- init mail

local player_id, server_id = ARGV[1], ARGV[2]
if player_id <= 0 or server_id <= 0 then
	return {100, 'invalid player_id or server_id'}
end

local player_key = 'player:' .. player_id
redis.call('hset', player_key, 'server_id', server_id)
redis.call('expire', player_key, 86400*35)
redis.call('srem', 'serverplayer:'..server_id, player_id)
return {0, 'OK'}




