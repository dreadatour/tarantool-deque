local function index_unconfigured()
    box.error(box.error.PROC_LUA, "Please run box.cfg{} first")
end

local deque = {}
setmetatable(deque, {__index = index_unconfigured})

if rawget(box, 'space') == nil then
    local orig_cfg = box.cfg
    box.cfg = function(...)
        local result = {orig_cfg(...)}

        local abstract = require('deque.abstract')
        for k, v in pairs(abstract) do
            rawset(deque, k, v)
        end
        setmetatable(deque, getmetatable(abstract))
        deque.start()

        return unpack(result)
    end
else
    deque = require('deque.abstract')
end

return deque
