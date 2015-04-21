local fiber = require('fiber')
local json = require('json')
local log = require('log')

-- maximum timeout in seconds (500 years will be enough for everyone :)
local TIMEOUT_INFINITY  = 365 * 86400 * 500
-- task states
local state = {
    DELAYED = '~',  -- task is delayed (waiting)
    READY   = 'r',  -- task is ready for work
    TAKEN   = 't',  -- task is taken by worker
    DONE    = '-',  -- task is done
}

-- space schema
local i_id         = 1  -- task ID
local i_status     = 2  -- task status
local i_next_event = 3  -- time of next event to be done on this task
local i_ttl        = 4  -- time to live
local i_created    = 5  -- creation time
local i_data       = 6  -- task data

local tube = {}
local method = {}


-- create space
function tube.create_space(space_name)
    -- 1        2       3           4    5,       6
    -- task_id, status, next_event, ttl, created, data
    local space = box.schema.create_space(space_name)

    space:create_index('task_id', {type = 'tree', parts = {i_id, 'num'}})
    space:create_index('status',  {type = 'tree', parts = {i_status, 'str', i_id, 'num'}})
    space:create_index('watch',   {type = 'tree', parts = {i_status, 'str', i_next_event, 'num'}})

    return space
end


-- start tube on space
function tube.new(space, on_task_change, opts)
    if on_task_change == nil then
        on_task_change = function() end
    end

    local self = {
        space = space,
        opts = opts,
        on_task_change = function(self, task, stats_data)
            -- wakeup fiber
            if task ~= nil and self.fiber ~= nil and self.fiber:id() ~= fiber.id() then
                self.fiber:wakeup()
            end
            on_task_change(task, stats_data)
        end
    }
    setmetatable(self, {__index = method})

    self.fiber = fiber.create(self._fiber, self)

    return self
end


-- watch fiber
function method._fiber(self)
    local estimated, now, task

    fiber.name('deque')
    log.info("Started deque fiber")

    while true do
        estimated = TIMEOUT_INFINITY
        now = 0ULL + fiber.time() * 1000000

        -- delayed tasks
        task = self.space.index.watch:min{state.DELAYED}
        if task ~= nil and task[i_status] == state.DELAYED then
            if now >= task[i_next_event] then
                task = self.space:update(task[i_id], {
                    {'=', i_status, state.READY},
                    {'=', i_next_event, task[i_ttl]}
                })
                self:on_task_change(task)
                estimated = 0
            else
                local et = tonumber(task[i_next_event] - now) / 1000000
                if et < estimated then
                    estimated = et
                end
            end
        end

        -- ttl tasks
        task = self.space.index.watch:min{state.READY}
        if task ~= nil and task[i_status] == state.READY then
            if now >= task[i_ttl] then
                self.space:delete(task[i_id])
                self:on_task_change(task:transform(2, 1, state.DONE))
                estimated = 0
            else
                local et = tonumber(task[i_next_event] - now) / 1000000
                if et < estimated then
                    estimated = et
                end
            end
        end

        if estimated > 0 then
            task = nil  -- free refcounter
            fiber.sleep(estimated)
        end
    end
end


-- cleanup internal fields in task
function method.normalize_task(self, task)
    if task ~= nil then
        return task:transform(3, 3)
    end
end


-- put task in space
function method.put(self, data, opts)
    local id = 0
    local max = self.space.index.task_id:max()
    if max ~= nil then
        id = max[i_id] + 1
    end

    local ttl = opts.ttl or TIMEOUT_INFINITY

    local now = fiber.time()
    local valid_until = 0ULL + (now + ttl) * 1000000
    local status = state.READY
    local next_event = valid_until

    -- TODO: check if delay > ttl
    if opts.delay ~= nil and opts.delay > 0 then
        status = state.DELAYED
        next_event = 0ULL + (now + opts.delay) * 1000000
    end

    local task = self.space:insert{
        id,
        status,
        next_event,
        valid_until,
        now,
        data
    }
    self:on_task_change(task, 'put')

    return task
end


-- take task
function method.take(self)
    local task = self.space.index.status:min{state.READY}
    if task == nil or task[i_status] ~= state.READY then
        return
    end

    task = self.space:update(task[i_id], {
        {'=', i_status, state.TAKEN},
        {'=', i_next_event, task[i_ttl]}
    })
    self:on_task_change(task, 'take')

    return task
end


-- delete task
function method.delete(self, id)
    local task = self.space:delete(id)
    if task ~= nil then
        task = task:transform(2, 1, state.DONE)
    end
    self:on_task_change(task, 'delete')
    return task
end


-- release task
function method.release(self, id, opts)
    local task = self.space:get{id}
    if task == nil then
        return
    end

    if opts.delay ~= nil and opts.delay > 0 then
        task = self.space:update(id, {
            {'=', i_status, state.DELAYED},
            {'=', i_next_event, 0ULL + (fiber.time() + opts.delay) * 1000000},
        })
    else
        task = self.space:update(id, {
            {'=', i_status, state.READY},
            {'=', i_next_event, task[i_ttl]},
        })
    end
    self:on_task_change(task, 'release')

    return task
end


-- peek task
function method.peek(self, id)
    return self.space:get{id}
end


return tube
