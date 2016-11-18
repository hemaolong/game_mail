local cjson = require 'cjson'
local conf = require 'conf_server'
local redis = require "resty.redis":new()

local mail = {}

function mail.init(data)
	local player_id, server_id = ARGV[1], ARGV[2]
	if player_id <= 0 or server_id <= 0 then
		return {100, 'invalid player_id or server_id'}
	end

	local player_key = 'player:' .. player_id
	redis.call('hset', player_key, 'server_id', server_id)
	redis.call('expire', player_key, 86400*35)
	redis.call('srem', 'serverplayer:'..server_id, player_id)
	return {0, 'OK'}
end

function mail.get(data)
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

	local server_mails = redis.call('zrangebyscore', 'servermail:' .. server_id, now, version+1, 'limit', 0, 30)
	redis.call('zadd', player_key, 'version', now)
	for _, v in ipairs(server_mails) do
		local send_ts = redis.call('hget', v, 'ts')
		if send_ts then
			redis.call('zadd', player_key, 'NX', v, send_ts)
		end
	end
end

function mail.delete(data)
	local player_id, mail_id = ARGV[1], ARGV[2]
	if player_id == 0 or not mail_id then return {300, 'error: 1'} end
	local player_key = 'player:'..player_id
	if redis.call('hdel', player_key, mail_id) == 0 then
		return {301, 'error: player have no mail '.. player_id .. ' ' .. mail_id}
	end
	if redis.call('del', mail_id) then
		return {302, 'error: mail not exist '.. mail_id}
	end
	return {0}
end

function mail.send(data)
  local mail_data = cjson.decode(data)
	if not mail_data then
		return 200, 'invalid mail data'
	end

	local server_id, player_id, form_id, title, content, atts, count = 
	  mail_data.server_id, mail_data.player_id, mail_data.form_id, mail_data.title, mail_data.content, mail_data.atts, mail_data.count

	if server_id <= 0 and player_id <= 0 then
	  return {201, 'invalid params'} end
	end

	local mail_params = {'ts', ts}
	if form_id > 0 then
		mail_params[#mail_params+1] = 'mail_id'
		mail_params[#mail_params+1] = mail_id
	end
	if title and #title > 0 then
		mail_params[#mail_params+1] = 'title'
		mail_params[#mail_params+1] = title
	end
	if content and #content > 0 then
		mail_params[#mail_params+1] = 'content'
		mail_params[#mail_params+1] = content
	end
	if atts and #atts > 0 then
		mail_params[#mail_params+1] = 'atts'
		mail_params[#mail_params+1] = atts
	end
	-- if atts_mask and atts_mask > 0 then
	-- 	mail_params[#mail_params+1] = 'atts_mask'
	-- 	mail_params[#mail_params+1] = atts_mask
	-- end
	if count > 0 then
		mail_params[#mail_params+1] = 'count'
	    mail_params[#mail_params+1] = count
	end
	if #mail_params <= 0 then return {201, 'invalid params'} end

	local mail_key = 'mail:'..player_id..':'..new_mail_id
	redis.call('hmset', mail_key, unpack(mail_params))
	redis.call('expire', mail_key, 86400*35)
	if player_id <= 0 then
		-- Broadcast mails, for all server player
		redis.call('zadd', 'servermail:' .. server_id, ts, mail_key) -- Used to notify player
	else
		local new_mail_id = redis.call('incr', 'mail_id')
		-- Normal mails
		redis.call('sadd', 'player:' .. player_id, mail_key, ts)
		redis.call('sadd', 'serverplayer:'..server_id, player_id) -- Used to notify player
	end

	return {0}
end


return mail
