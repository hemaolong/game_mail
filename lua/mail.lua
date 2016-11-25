local cjson = require 'cjson'
local conf = require 'conf_server'
local redis = require "resty.redis"

local mail = {}

local _mail_count_max = 60
local _mail_expire_secs = 86400*30

local function _get_mail_redis()
	local red = redis:new()
  red:set_timeout(1000*5)
	local redis_conf = conf.redis.mail
	local ok, err = red:connect(redis_conf.host, redis_conf.port)
	if not ok then
		return
	end
	return red
end
local function _get_player_key(player_id)
	return 'player:' .. player_id
end

local function _get_player_mail_version_key()
	return 'player_version'
end

local function _add_mail_to_player(mail_id, player_id, ts)
end

local function _get_server_broadcast_mail_id(server_id)
	return 'mail_broadcast:' .. server_id
end

local _fill_script = [[
local server_id, player_id, now, expire_secs = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local version = redis.call('hget', 'mail_version', player_id) or now
version = tonumber(version)
local pk = 'player:'..player_id
redis.call('hset', 'mail_version', player_id, now)
local b = now - expire_secs
if version < b then
	version = b
end
local mails = redis.call('zrevrangebyscore', 'mail_broadcast'..server_id, now, version, 'withscores')
for i = 1, #mails/2 do
  local mail_id, send_ts = mails[2*i-1], mails[2*i]
  redis.call('zadd', player_key, mail_id, send_ts)
end
return true
]]
local function _fill_player_server_broadcast_mails(red, server_id, player_id, now)
	local ret, err = red:eval(_fill_script, 0, server_id, player_id, now, _mail_expire_secs)
	if ret ~= 1 then
		ngx.log(ngx.ERR, 'fill mail error ', err)
		return 202, err
	end
	return true
end

-- function mail.init(data)
-- 	local player_id, server_id = data.player_id, data.server_id
-- 	if player_id <= 0 or server_id <= 0 then
-- 		return {100, 'invalid player_id or server_id'}
-- 	end

-- 	local player_key = _get_player_key(player_id)
-- 	red:hset(player_key, 'server_id', server_id)
-- 	red:expire(player_key, 86400*35)
-- 	red:srem('serverplayer:'..server_id, player_id)
-- 	return 0, 'OK'
-- end

-- ENSURE the mail is self mail
local function _array2map(a)
  local m = {}
  if not a then return m end
  for i = 1, #a/2 do
		m[a[i*2-1]] = a[i*2]
	end
	return m
end

local function _resp_mail_list(red, player_id, mail_ids)
	if not mail_ids then
		return
	end
	local result = {player_id=player_id, mails={}}
	for k, v in ipairs(mail_ids) do
		local mail_attay = red:hgetall(v)
		local mail = _array2map(mail_attay)
		mail['mail_id'] = v
		result.mails[#result.mails+1] = mail
	end
	return 0, result
end

function mail.get(args)
  local data = cjson.decode(args)
	local player_id, server_id, get_all = data.player_id, data.server_id, data.get_all
	local now = ngx.time()
	if not player_id or player_id <= 0 then return 300, 'invalid player_id' end

	local player_key = _get_player_key(player_id)

	local red = _get_mail_redis()
	if get_all then
		-- Do send server-broadcast mails
		--_add_mail_to_player(_get_player_key(player_id), mail_id, now)
		if server_id then
		  _fill_player_server_broadcast_mails(red, server_id, player_id, now)
		end

    local b = now - _mail_expire_secs
		local ret, err = red:zrevrangebyscore(player_key, '+inf', b, 'limit', 0, _mail_count_max-1)
		if not ret then
			ngx.log(ngx.ERR, 'get player mail error ', err)
		end
		return _resp_mail_list(red, player_id, ret)
	end

	-- Only get the new mails
	return _resp_mail_list(red, player_id, red:zrangebyscore('mail_pending:'..server_id, player_id, player_id))
end

function mail.delete(args)
	local data = cjson.decode(args)
	local player_id, mail_id = data.player_id, data.mail_id
	if player_id == 0 or not mail_id then return {300, 'error: 1'} end
	local player_key = _get_player_key(player_id)
	if red:hdel(player_key, mail_id) == 0 then
		return 220, 'error: player have no mail '.. player_id .. ' ' .. mail_id
	end
	if red:del(mail_id) then
		return 221, 'error: mail not exist '.. mail_id
	end
	return 0
end


local _read_script = [[
  local player_key, mail_id = ARGV[1], ARGV[2]
  local ms = redis.call('zscore', player_key, mail_id)
  if not ms then
   return {false, 'mail thief'.. mail_id}
  end
  local exist = redis.call('exists', mail_id)
  if not exist then
  	return {false, 'mail miss ' .. mail_id}
  end
  if redis.call('hsetnx', mail_id, 'read', 1) ~= 1 then
  	return {false, 'already read'}
  end
  return {true, 1}
]]
function mail.read(args)
	local data = cjson.decode(args)
	local player_id, mail_id = data.player_id, data.mail_id
	if not (player_id and mail_id) then
		return 230, 'invalid args'
	end
	local player_key = _get_player_key(player_id)
	local red = _get_mail_redis()
	local eval_ret, err = red:eval(_read_script, 0, player_key, mail_id)
	if not eval_ret then
		return 232, err
	end
	local ret, err = eval_ret[1], eval_ret[2]
	if ret ~= 1 then -- Yes, compare with 1!!!
		return 232, err
	end
	ngx.log(ngx.INFO, 'read mail ', player_id, ' ', mail_id, ' ', tostring(type(ret)))
	return 0, {player_id=player_id, mail_id=mail_id}
end


local _gain_script = [[
  local player_key, mail_id, att_index = ARGV[1], ARGV[2], ARGV[3]
  local ms = redis.call('zscore', player_key, mail_id)
  if not ms then
   return {false, 'mail thief'}
  end
  if not redis.call('exists', mail_id) then
  	return {false, 'mail miss'}
  end
  local f = tonumber(redis.call('hget', mail_id, 'gain_flag') or 0)
  local m = bit.lshift(1, att_index)
  if bit.band(f, m) ~= 0 then
  	return {false, 'already get ' .. mail_id}
  end
  local nf = bit.bor(f, m)
  redis.call('hset', mail_id, 'gain_flag', nf)
  local mail = redis.call('hgetall', mail_id)
  return {true, false, nf, mail}
]]
function mail.get_atts(args)
	local data = cjson.decode(args)
	local player_id, mail_id, att_index = data.player_id, data.mail_id, data.att_index
	if not (player_id and mail_id) then
		return 240, 'invalid args'
	end
	if att_index < 0 or att_index > 30  then
		return 241, 'invalid att index'
	end
	local player_key = _get_player_key(player_id)
	local red = _get_mail_redis()
	local eval_ret, err = red:eval(_gain_script, 0, player_key, mail_id, att_index)
	if not eval_ret then
		return 232, err
	end
	local ret, err, gain_flag, mail_data = eval_ret[1], eval_ret[2], eval_ret[3], eval_ret[4]
	if ret ~= 1 then -- Yes, compare with 1!!!
		return 242, err
	end
	-- TODO assert (gain_flag & (1 << att_index)) == 1
	mail_data[#mail_data+1] = 'mail_id'
	mail_data[#mail_data+1] = mail_id
	local mail_msg = _array2map(mail_data)
	ngx.log(ngx.INFO, 'get mail atts| ', player_id, ' ', mail_id, ' ', gain_flag)
	return 0, {player_id=player_id, att_index=att_index, mail=mail_msg}
end


local _allow_mail_fields = {
	'form_id',
	'title',
	'content',
	'atts',
	'ultra_params',
	'log_source',
}

function mail.send(args)
  local mail_data = cjson.decode(args)
	if not mail_data then
		return 200, 'invalid mail data'
	end

  local now = ngx.time()
	local player_id, server_id = mail_data.player_id, mail_data.server_id

	if player_id <= 0 then
	  return 201, 'invalid params'
	end

	local mail_params = {'ts', now}
	for _, v in ipairs(_allow_mail_fields) do
	  local f = mail_data[v]
	  if f then
	  	mail_params[#mail_params+1] = v
	  	mail_params[#mail_params+1] = f
	  end
	end
	if #mail_params <= 0 then return 201, 'invalid params' end

  -- Do save to redis
	local red = _get_mail_redis()
	local mail_index = red:incr('mail_index')  
	local player_key = _get_player_key(player_id)
	local mail_id = 'mail:'..player_id..':'..mail_index
	red:hmset(mail_id, unpack(mail_params))
	red:expire(mail_id, _mail_expire_secs)
	if player_id <= 0 then
		-- Broadcast mails, for all server player
		if server_id <= 0  then
			return 203, 'miss server id'
		end
		red:zadd(_get_server_broadcast_mail_id(server_id), now, mail_id) -- Used to notify player
		ngx.log(ngx.INFO, 'send mail to server| ', args)
	else
		-- Normal mails
		--red:sadd()
		red:zadd(player_key, now, mail_id)
		if server_id then
			red:zadd('mail_pending:'..server_id, player_id, mail_id) -- Used to notify player
		end
		ngx.log(ngx.INFO, 'send mail to player| ', args)
	end

	return 0, {mail_id=mail_id}
end


return mail
