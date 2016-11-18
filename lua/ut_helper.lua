
local ut = {}
local cjson = require 'cjson'
function ut.wrap_resp(error_code, data)
	return cjson.encode({err=error_code, data=data})
end

return ut