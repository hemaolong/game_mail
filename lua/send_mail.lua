 -- send mail
 -- mail:pid:mid{server_id, for_all, player_id, form_id, title, content, atts, atts_mask, ts}
 -- player:pid{version, }
local server_id, player_id, form_id, title, content, atts, atts_mask, count, ts = 
  ARGV[1], ARGV[2], ARGV[3],ARGV[4],ARGV[5],ARGV[6],ARGV[7],ARGV[8],ARGV[9]

if server_id <= 0 and player_id <= 0 then
  return {200, 'invalid params'} end
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
if atts_mask and atts_mask > 0 then
	mail_params[#mail_params+1] = 'atts_mask'
	mail_params[#mail_params+1] = atts_mask
end
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