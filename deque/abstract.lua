local fiber = require('fiber')
local log = require('log')
local json = require('json')
local session = box.session

-- maximum timeout in seconds (500 years will be enough for everyone :)
local TIMEOUT_INFINITY  = 365 * 86400 * 500

-- task states
local state = {
    DELAYED = '~',  -- task is delayed (waiting)
    READY   = 'r',  -- task is ready for work
    TAKEN   = 't',  -- task is taken by worker
    DONE    = '-',  -- task is done
}
local human_status = {}
human_status[state.READY]   = 'ready'
human_status[state.DELAYED] = 'delayed'
human_status[state.TAKEN]   = 'taken'
human_status[state.DONE]    = 'done'

-- space schema
local i_id         = 1  -- task ID
local i_status     = 2  -- task status
local i_next_event = 3  -- time of next event to be done on this task
local i_ttl        = 4  -- time to live
local i_created    = 5  -- creation time
local i_data       = 6  -- task data


local queue = {
    tube = {},
    stat = {},
}
local tube = {}
local method = {}


-- cleanup internal fields in task
function tube.normalize_task(self, task)
    if task ~= nil then
        return task:transform(3, 3)
    end
end

-- put task in space
function tube.put(self, data, opts)
    if opts == nil then
        opts = {}
    end

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

    return self:normalize_task(task)
end


-- take task
function tube.take(self, timeout)
    if timeout == nil then
        timeout = TIMEOUT_INFINITY
    end

    -- FIXME: duplicate code below
    local task = self.space.index.status:min{state.READY}
    if task ~= nil and task[i_status] == state.READY then
        task = self.space:update(task[i_id], {
            {'=', i_status, state.TAKEN},
            {'=', i_next_event, task[i_ttl]}
        })
        self:on_task_change(task, 'take')
        return self:normalize_task(task)
    end

    local started, time, tube_id
    while timeout > 0 do
        started = fiber.time()
        time = 0ULL + (started + timeout) * 1000000
        tube_id = self.tube_id

        box.space._queue_consumers:insert{session.id(), fiber.id(), tube_id, time, started}
        fiber.sleep(timeout)
        box.space._queue_consumers:delete{session.id(), fiber.id()}

        -- FIXME: duplicate code above
        local task = self.space.index.status:min{state.READY}
        if task ~= nil and task[i_status] == state.READY then
            task = self.space:update(task[i_id], {
                {'=', i_status, state.TAKEN},
                {'=', i_next_event, task[i_ttl]}
            })
            self:on_task_change(task, 'take')
            return self:normalize_task(task)
        end

        timeout = timeout - (fiber.time() - started)
    end
end

-- ack task
function tube.ack(self, id)
    local _taken = box.space._queue_taken:get{session.id(), self.tube_id, id}
    if _taken == nil then
        box.error(box.error.PROC_LUA, "Task was not taken in the session")
    end

    self:peek(id)
    box.space._queue_taken:delete{session.id(), self.tube_id, id}
    return self:delete(id)
end

-- release task
function tube.release(self, id, opts)
    if opts == nil then
        opts = {}
    end

    local _taken = box.space._queue_taken:get{session.id(), self.tube_id, id}
    if _taken == nil then
        box.error(box.error.PROC_LUA, "Task was not taken in the session")
    end

    box.space._queue_taken:delete{session.id(), self.tube_id, id}
    self:peek(id)

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

    return self:normalize_task(task)
end

-- peek task
function tube.peek(self, id)
    local task = self.space:get{id}
    if task == nil then
        box.error(box.error.PROC_LUA, string.format("Task %s not found", tostring(id)))
    end

    return self:normalize_task(task)
end

-- delete task
function tube.delete(self, id)
    self:peek(id)

    local task = self.space:delete(id)
    if task ~= nil then
        task = task:transform(2, 1, state.DONE)
    end
    self:on_task_change(task, 'delete')

    return self:normalize_task(task)
end

-- drop tube
function tube.drop(self)
    local tube_name = self.name

    local tube = box.space._queue:get{tube_name}
    if tube == nil then
        box.error(box.error.PROC_LUA, "Tube not found")
    end

    local tube_id = tube[2]

    local cons = box.space._queue_consumers.index.consumer:min{tube_id}

    if cons ~= nil and cons[3] == tube_id then
        box.error(box.error.PROC_LUA, "There are consumers connected the tube")
    end

    local taken = box.space._queue_taken.index.task:min{tube_id}
    if taken ~= nil and taken[2] == tube_id then
        box.error(box.error.PROC_LUA, "There are taken tasks in the tube")
    end

    local space_name = tube[3]

    box.space[space_name]:drop()
    box.space._queue:delete{tube_name}
    queue.tube[tube_name] = nil

    return true
end

-- watch fiber
function tube._fiber(self)
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


-- methods
local function make_self(space, tube_name, tube_type, tube_id, opts)
    if opts == nil then
        opts = {}
    end

    local self

    -- wakeup consumer if queue have new task
    local on_task_change = function(task, stats_data)
        -- task was removed
        if task == nil then
            return
        end

        -- if task was taken and become other state
        local taken = box.space._queue_taken.index.task:get{tube_id, task[1]}
        if taken ~= nil then
            box.space._queue_taken:delete{taken[1], taken[2], taken[3]}
        end

        -- task swicthed to ready (or new task)
        if task[2] == state.READY then
            local tube_id = self.tube_id
            local consumer = box.space._queue_consumers.index.consumer:min{tube_id}

            if consumer ~= nil then
                if consumer[3] == tube_id then
                    fiber.find(consumer[2]):wakeup()
                    box.space._queue_consumers:delete{consumer[1], consumer[2]}
                end
            end

        -- task swicthed to taken - registry in taken space
        elseif task[2] == state.TAKEN then
            box.space._queue_taken:insert{session.id(), self.tube_id, task[1], fiber.time()}
        end

        if stats_data ~= nil then
            queue.stat[space.name]:inc(stats_data)
        end
    end

    self = {
        name    = tube_name,
        type    = tube_type,
        tube_id = tube_id,
        space   = space,
        opts    = opts,
        on_task_change = function(self, task, stats_data)
            -- wakeup fiber
            if task ~= nil and self.fiber ~= nil and self.fiber:id() ~= fiber.id() then
                self.fiber:wakeup()
            end
            on_task_change(task, stats_data)
        end
    }
    setmetatable(self, {__index = tube})

    self.fiber = fiber.create(self._fiber, self)

    queue.tube[tube_name] = self

    return self
end

function method._on_consumer_disconnect()
    local waiter, fb, task, tube, id
    id = session.id()

    -- wakeup all waiters
    while true do
        waiter = box.space._queue_consumers.index.pk:min{id}
        if waiter == nil then
            break
        end

        box.space._queue_consumers:delete{waiter[1], waiter[2]}

        fb = fiber.find(waiter[2])
        if fb ~= nil and fb:status() ~= 'dead' then
            fb:wakeup()
        end
    end

    -- release all session tasks
    while true do
        task = box.space._queue_taken.index.pk:min{id}
        if task == nil or task[1] ~= id then
            break
        end

        tube = box.space._queue.index.tube_id:get{task[2]}
        if tube == nil then
            log.error("Inconsistent queue state: tube %d not found", task[2])
            box.space._queue_taken:delete{task[1], task[2], task[3]}
        else
            log.warn("Consumer %s disconnected, release task %s(%s)", id, task[3], tube[1])
            queue.tube[tube[1]]:release(task[3])
        end
    end
end

-- create tube
function method.create_tube(tube_name, opts)
    if opts == nil then
        opts = {}
    end

    -- space name must be equal to tube name
    -- https://github.com/tarantool/queue/issues/9#issuecomment-83019109
    local space_name = tube_name
    if box.space[space_name] ~= nil then
        box.error(box.error.PROC_LUA, "Space " .. space_name .. " already exists")
    end

    -- create tube record
    local last = box.space._queue.index.tube_id:max()
    local tube_id = 0
    if last ~= nil then
        tube_id = last[2] + 1
    end
    box.space._queue:insert{tube_name, tube_id, space_name, tube_type, opts}

    -- create tube space
    -- 1        2       3           4    5,       6
    -- task_id, status, next_event, ttl, created, data
    local space = box.schema.create_space(space_name)
    space:create_index('task_id', {type = 'tree', parts = {i_id, 'num'}})
    space:create_index('status',  {type = 'tree', parts = {i_status, 'str', i_id, 'num'}})
    space:create_index('watch',   {type = 'tree', parts = {i_status, 'str', i_next_event, 'num'}})

    return make_self(space, tube_name, tube_type, tube_id, opts)
end

-- create or join infrastructure
function method.start()
    local _queue = box.space._queue
    if _queue == nil then
        -- tube_name, tube_id, space_name, tube_type, opts
        _queue = box.schema.create_space('_queue')
        _queue:create_index('tube', {type = 'tree', parts = {1, 'str'}})
        _queue:create_index('tube_id', {type = 'tree', parts = {2, 'num'}, unique = true})
    end

    local _cons = box.space._queue_consumers
    if _cons == nil then
        -- session, fid, tube, time
        _cons = box.schema.create_space('_queue_consumers', {temporary = true})
        _cons:create_index('pk', {type = 'tree', parts = {1, 'num', 2, 'num'}})
        _cons:create_index('consumer', {type = 'tree', parts = {3, 'num', 4, 'num'}})
    end

    local _taken = box.space._queue_taken
    if _taken == nil then
        -- session_id, tube_id, task_id, time
        _taken = box.schema.create_space('_queue_taken', {temporary = true})
        _taken:create_index('pk', {type = 'tree', parts = {1, 'num', 2, 'num', 3, 'num'}})
        _taken:create_index('task', {type = 'tree', parts = {2, 'num', 3, 'num'}})
    end

    for _, tube_rc in _queue:pairs() do
        local tube_name     = tube_rc[1]
        local tube_id       = tube_rc[2]
        local tube_space    = tube_rc[3]
        local tube_type     = tube_rc[4]
        local tube_opts     = tube_rc[5]

        local space = box.space[tube_space]
        if space == nil then
            box.error(box.error.PROC_LUA, "Space " .. tube_space .. " is not exists")
        end
        make_self(space, tube_name, tube_type, tube_id, tube_opts)
    end

    session.on_disconnect(queue._on_consumer_disconnect)

    return queue
end


local idx_tube = 1

local function put_statistics(stat, space, tube)
    if space == nil then
        return
    end

    local st = rawget(queue.stat, space)
    if st == nil then
        return
    end

    local space_stat = {}
    space_stat[space] = {tasks={}, calls={}}

    -- add api calls stats
    for name, value in pairs(st) do
        if type(value) ~= 'function' then
            local s_table = {}
            s_table[tostring(name)] = value
            table.insert(space_stat[space]['calls'], s_table)
        end

    end

    -- add total tasks count
    local s_total = {}
    s_total['total'] = box.space[space].index[idx_tube]:count()
    table.insert(space_stat[space]['tasks'], s_total)

    -- add tasks by state count
    for i, s in pairs(state) do
        local s_table = {}
        s_table[human_status[s]] = box.space[space].index[idx_tube]:count(s)
        table.insert(space_stat[space]['tasks'], s_table)
    end
    table.insert(stat, space_stat)
end

queue.statistics = function(space)
    local stat = {}

    if space ~= nil then
        put_statistics(stat, space)
    else
        for space, spt in pairs(queue.stat) do
            put_statistics(stat, space)
        end
    end

    return stat

end

setmetatable(queue.stat, {
    __index = function(tbs, space)
        local spt = {
            inc = function(t, cnt)
                t[cnt] = t[cnt] + 1
                return t[cnt]
            end
        }
        setmetatable(spt, {
            __index = function(t, cnt)
                rawset(t, cnt, 0)
                return 0
            end
        })

        rawset(tbs, space, spt)
        return spt
    end,
    __gc = function(tbs)
        for space, tubes in pairs(tbs) do
            for tube, tbt in pairs(tubes) do
                rawset(tubes, tube, nil)
            end
            rawset(tbs, space, nil)
        end
    end
})

setmetatable(queue, {__index = method})
return queue
