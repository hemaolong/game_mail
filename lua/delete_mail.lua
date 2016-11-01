
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
