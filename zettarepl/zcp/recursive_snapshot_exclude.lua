local function starts_with(str, start)
   return str:sub(1, #start) == start
end

args = ...
dataset = args["argv"][1]
snapshot_name = args["argv"][2]
exclude_filename = args["argv"][3]
end

excl_file = io.open(exclude_filename, "r")
snapshots_to_create = {}
function populate_snapshots_to_create(dataset, exclude_file)
    table.insert(snapshots_to_create, dataset .. "@" .. snapshot_name)

    local child_iterator = zfs.list.children(dataset)
    while true do
        local child = child_iterator()
        if child == nil then
            break
        end

        local exclude_iterator = exclude_file:lines()

        local include = true
        for exclude_line in exclude_iterator do
            local exclude = string.gsub(exclude_line, "[\n]", "")
            if child == excl then
                include = false
                break
            end
        end
        if include then
            populate_snapshots_to_create(child)
        end
    end
end
populate_snapshots_to_create(dataset, excl_file)
excl_file:close()

errors = {}
for _, snapshot in ipairs(snapshots_to_create) do
    local error = zfs.check.snapshot(snapshot)
    if (error ~= 0) then
        table.insert(errors, "snapshot=" .. snapshot .. " error=" .. tostring(error))
    end
end

if (#errors ~= 0) then
    error(table.concat(errors, ", "))
end

for _, snapshot in ipairs(snapshots_to_create) do
    assert(zfs.sync.snapshot(snapshot) == 0)
end
