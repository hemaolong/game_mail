local cjson = require 'cjson'
local conf = require 'conf_server'
local redis = require "resty.redis"

local _max_gm_commands_count = 100

local gm = {}


local _shared_gm_poll = ngx.shared.shared_gm_poll
local function _set_pending_dirty(server_id, dirty)
  	if dirty then
    	_shared_gm_poll:set(server_id, true)
	else
		_shared_gm_poll:delete(server_id)
	end
end
local function _get_pending_dirty(server_id, dirty)
  return _shared_gm_poll:get(server_id)
end

local function _get_gm_redis()
	local red = redis:new()
  red:set_timeout(1000*5)
	local redis_conf = conf.redis.gm
	local ok, err = red:connect(redis_conf.host, redis_conf.port)
	if not ok then
		return
	end
	return red
end

local function _get_gm_key(server_id)
	return 'server_commands:' .. server_id
end

local function _get_operation_id(red, now)
	local version = red:incr('gm_command_version')
	return tostring(now) .. ':' .. version
end

local function _init_operation_result(red, operation_id, command_count)
	red:hset('gm_command_result:'..operation_id, 'init_count', command_count)
end

local function _incr_operation_result_success_count(red, operation_id)
	red:hincrby('gm_command_result:'..operation_id, 'success', 1)
end

local function _incr_operation_result_fail_count(red, operation_id)
	red:hincrby('gm_command_result:'..operation_id, 'fail', 1)
end

function gm.add_result(args)
	local data = cjson.decode(args)
	local red = _get_gm_redis()
	for _, v in ipairs(data) do
		local operation_id, ok, error_msg = v.operation_id, v.ok, v.error_msg
		if ok then
			_incr_operation_result_success_count(red, operation_id)
		else
			_incr_operation_result_fail_count(red, operation_id)
		end
	end
end

function gm.add(args)
	local data = cjson.decode(args)
	local red = _get_gm_redis()
	local now = ngx.time()
	local operation_id = _get_operation_id(red, now)
	_init_operation_result(operation_id)
	for _, v in ipairs(data) do
		local server_id, command = v.server_id, v.command
		local ok, err = red:rpush(_get_gm_key(server_id), cjson.encode{operation_id=operation_id, command=command})
		if not ok then
			ngx.log(ngx.ERR, 'fail to send gm command ', err)
			return 301, 'fail to append commands ' .. server_id
		end
	end
	return 0, 'operation_id'
end

local function _pop_pending_gms(red, server_id)
	red:multi()
	local gm_key = _get_gm_key(server_id)
    red:lrange(gm_key, 0, _max_gm_commands_count-1)
    red:ltrim(gm_key, 0, _max_gm_commands_count-1)
    local result, err = red:exec()
	if not result then
		ngx.log(ngx.ERR, 'fail to send gm command ', err)
		return
	end

	return 0, result[1]
end

function gm.poll(server_id)
	if not _get_pending_dirty(server_id) then
		return
	end
	local red = _get_gm_redis()
	local result = _pop_pending_gms(red, server_id)
	if not result or #result == 0 then return end
	return result
end


return gm
