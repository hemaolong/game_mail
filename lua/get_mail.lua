
local player_id = ARGV[1]
local now = ARGV[2]
if not player_id or player_id <= 0 then return {300, 'invalid player_id'} end

local player_key = 'player:'..player_id
local result = redis.call('hmget', player_key, 'server_id', 'version')
local server_id, version = result[1], result[2] or now

if not server_id then
	return {301, 'invalid player server'}
end

local begin = now = 86400 * 20
if version < begin then
	version = begin
end

local server_mails = redis.call('zrangebyscore', 'server:' .. player_id, now, version+1, 'limit', 0, 30)
redis.call('zadd', player_key, 'version', now)
for _, v in ipairs(server_mails) do
	local send_ts = redis.call('hget', v, 'ts')
	if send_ts then
		redis.call('zadd', player_key, 'NX', v, send_ts)
	end
end


