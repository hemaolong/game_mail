
-- Game Mail System 
-- @heml 2016.11.26
local cjson = require 'cjson'
local conf = require 'conf_server'
local redis = require "resty.redis"
local tonumber = tonumber
local unpack = unpack

local mail = {}

local _mail_count_max = 60
local _mail_expire_secs = 86400*30
local _mail_expire_ultra_seconds = 86400*5
local _get_pending_mail_max = 50

local function _get_mail_redis()
	local red = redis:new()
  red:set_timeout(1000*5)
	local redis_conf = conf.redis.mail
	local ok, err = red:connect(redis_conf.host, redis_conf.port)
	if not ok then
		return
	end
	--red:set_keepalive(1000*300, 100)
	return red
end
local function _get_player_key(player_id)
	return 'player:' .. player_id
end

local function _get_server_broadcast_mail_id(server_id)
	return 'server:mails:' .. server_id
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
local mails = redis.call('zrevrangebyscore', 'server:mails:'..server_id, now, version+1)
local result = {}
for _, v in ipairs(mails) do
  local mail_dump = redis.call('dump', v)
  if mail_dump then
	  result[#result+1] = {v, mail_dump}
	end
end
return result
]]
local function _fill_player_server_broadcast_mails(red, server_id, player_id, now)
	local mail_dump_list, err = red:eval(_fill_script, 0, server_id, player_id, now, _mail_expire_secs)
	if not mail_dump_list then
		ngx.log(ngx.ERR, 'fill mail error ', err)
		return nil, err
	end
	local player_key = _get_player_key(player_id)
	for _, v in ipairs(mail_dump_list) do
		local boradcast_id, broadcast_body = v[1], v[2]
		local mail_index = red:incr('mail_index')  
	  local mail_id = 'mail:'..player_id..':'..mail_index
	  red:restore(mail_id, _mail_expire_secs*1000, broadcast_body)
	  red:hset(mail_id, 'boradcast_id', boradcast_id)
	  red:zadd(player_key, now, mail_id)
		ngx.log(ngx.INFO, 'send mail to player single|', player_id, ' ', server_id, ' ', mail_id, ' ', boradcast_id)
		ngx.log(ngx.INFO, 'send mail to player broadcast|', player_id, ' ', server_id, ' ', mail_id, ' ', boradcast_id)
	end

	return true
end

local function _get_pending_pids_key(server_id)
  return 'mail_pending:' .. server_id
end

function mail.init_server(args)
  local data = cjson.decode(args)
	local server_id = data.server_id
	if server_id <= 0 then
		return {222, 'invalid server_id'}
	end

	local pending_key = _get_pending_pids_key(server_id)
	local red = _get_mail_redis()
	red:del(pending_key)
	ngx.log(ngx.INFO, 'game server init ', server_id)

	return 0
end

-- ENSURE the mail is self mail
local function _array2map(a)
  local m = {}
  if not a then return m end
  for i = 1, #a/2 do
		m[a[i*2-1]] = a[i*2]
	end
	return m
end

local function _resp_mail_list(red, player_id, mail_ids, result_out)
	if not mail_ids then
		return
	end
	local result = result_out or {}
	for k, v in ipairs(mail_ids) do
		local mail_attay = red:hgetall(v)
		if mail_attay and #mail_attay > 0 then
			local mail = _array2map(mail_attay)
			mail['mail_id'] = v
			mail['player_id'] = player_id
			result[#result+1] = mail
		end
	end
	return result
end
local function _resp_mail_multi_player_list(red, mail_ids)
	if not mail_ids then
		return
	end
	local result = {}
	for i = 1, #mail_ids/2 do
		local mail_id, player_id = mail_ids[2*i-1], mail_ids[2*i]
		local mail_attay = red:hgetall(mail_id)
		if mail_attay and #mail_attay > 0 then
			local mail = _array2map(mail_attay)
			mail['mail_id'] = mail_id
			mail['player_id'] = player_id
			result[#result+1] = mail
		end
	end
	return 0, {mails=result}
end

local function _remove_player_from_pending(red, server_id, player_id)
	red:srem(_get_pending_pids_key(server_id), player_id)
end

local function _get_player_mails(red, now, player_id, server_id)
	if not player_id or player_id <= 0 then return 300, 'invalid player_id' end
	local player_key = _get_player_key(player_id)
	-- Trim old mails
	-- Reset the player data expire seconds
	red:zremrangebyscore(player_key, '-inf', now - (_mail_expire_secs+_mail_expire_ultra_seconds))    -- Trim old mails
	red:expire(player_key, _mail_expire_secs+_mail_expire_ultra_seconds)

	-- Do send server-broadcast mails
	if server_id then
	  _fill_player_server_broadcast_mails(red, server_id, player_id, now)
	end

  local b = now - _mail_expire_secs
	local ret, err = red:zrevrangebyscore(player_key, '+inf', b, 'limit', 0, _mail_count_max-1)
	if not ret then
		ngx.log(ngx.ERR, 'get player mail error ', err)
	end
	_remove_player_from_pending(red, server_id, player_id)
	local mails = _resp_mail_list(red, player_id, ret)
	return 0, {mails=mails}
end

local function _get_pending_mails(red, now, pids_list, server_id)
  if not server_id then return 301, 'invalid server id' end

  ngx.log(ngx.INFO, '_get_pending_mails ', cjson.encode(pids_list))

	local result = {}
    for _, v in ipairs(pids_list) do
			_fill_player_server_broadcast_mails(red, server_id, v, now)
      local version = tonumber(red:hget('mail_version', v)) or now
			local player_key = _get_player_key(v)
			local ret, err = red:zrevrangebyscore(player_key, now, version, 'limit', 0, _mail_count_max-1)
			if not ret then
				ngx.log(ngx.ERR, 'get player mail error ', err)
				return
			end
			local mails = _resp_mail_list(red, v, ret, result)
		end

	return 0, {mails=result}
end

function mail.get(args)
  local data = cjson.decode(args)
	local get_all, server_id = data.get_all, data.server_id
	local now = ngx.time()
	local red = _get_mail_redis()
	-- Get one player's all mail
	if get_all then
		local player_id = data.player_id
		return _get_player_mails(red, now, player_id, server_id)
	else
		-- Get multi players's pending mails
		local player_id_list = data.player_id_list
		return _get_pending_mails(red, now, player_id_list, server_id)
	end
end

-- function mail.get_pendings(args)
--   local data = cjson.decode(args)
-- 	local server_id = data.player_id, data.server_id, data.get_all

-- end

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
  if redis.call('exists', mail_id) == 0 then
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
  if redis.call('exists', mail_id) == 0 then
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

local function _generate_mail(red, prefix, player_id, now, mail_data, expire_secs)
	local mail_params = {ts =  now}
	for _, v in ipairs(_allow_mail_fields) do
	  local f = mail_data[v]
	  if f then
	  	mail_params[v] = f
	  end
	end

	local mail_index = red:incr('mail_index')  
	local mail_id = prefix..player_id..':'..mail_index
	red:hmset(mail_id, mail_params)
	if expire_secs and expire_secs > 0 then
		red:expire(mail_id, expire_secs)
	end
	return mail_id, mail_params
end

local _shared_mail_pool = ngx.shared.shared_mail_poll
local function _set_pending_dirty(server_id, dirty)
  	if dirty then
    	_shared_mail_pool:set(server_id, true)
	else
		_shared_mail_pool:delete(server_id)
	end
end
local function _get_pending_dirty(server_id, dirty)
  return _shared_mail_pool:get(server_id)
end

-- Normal mails
function mail.send(args)
  local mail_data = cjson.decode(args)
	if not mail_data then
		return 200, 'invalid mail data'
	end

	local player_id, server_id = mail_data.player_id, mail_data.server_id
	if not player_id then
		return 201, 'miss player id'
	end
  -- Do save to redis
	local red = _get_mail_redis()
	if not red then
		return 202, 'fail to create player mail| ' .. player_id .. '|' .. args
	end
  local player_key = _get_player_key(player_id)
	if red:exists(player_key) == 0 then
		return 203, 'player expired?'
	end
  local now = ngx.time()
	local mail_id = _generate_mail(red, 'mail:', player_id, now, mail_data, _mail_expire_secs + _mail_expire_ultra_seconds)
	-- Normal mails
	red:zadd(player_key, now, mail_id)
	if server_id then
		red:sadd(_get_pending_pids_key(server_id), player_id) -- Used to notify player
		-- notify game server
		-- channel:game:server_id
		red:publish('channel:game:' .. server_id, 'send_mail')
		_set_pending_dirty(server_id, true)
	end
	ngx.log(ngx.INFO, 'send mail to player single|', player_id, ' ', server_id, ' ', mail_id, ' ', args)

	return 0, {mail_id=mail_id,server_id=server_id}
end

-- Broadcast mail to all server's player
function mail.send_broadcast(args)
  local mail_data = cjson.decode(args)
	if not mail_data then
		return 200, 'invalid mail data'
	end

	local server_id = mail_data.server_id
	if not server_id then
		return 201, 'miss server id'
	end
  -- Do save to redis
	local red = _get_mail_redis()
	if not red then
		return 202, 'fail to create server mail| ' .. server_id .. '|' .. args
	end
  local now = ngx.time()
	local mail_id = _generate_mail(red, 'mailbroadcast:', server_id, now, mail_data)
		-- Broadcast mails, for all server player
	red:zadd(_get_server_broadcast_mail_id(server_id), now, mail_id) -- Used to notify player
	ngx.log(ngx.INFO, 'broadcast mail to server| ', args)

	return 0, {mail_id=mail_id,server_id=server_id}
end

function mail.poll(server_id)
	if not _get_pending_dirty(server_id) then
		return
	end

	local red = _get_mail_redis()
  local pending_key = _get_pending_pids_key(server_id)
	local pending_pids = red:srandmember(pending_key, _get_pending_mail_max-1)	
  if #pending_pids == 0 then
    _set_pending_dirty(server_id, false)
	  return
	end
  -- Only get the new mails
	red:srem(pending_key, unpack(pending_pids))
	return pending_pids
end

return mail
