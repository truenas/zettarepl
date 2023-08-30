snapshots_to_create = {}
function populate_snapshots_to_create(dataset)
    table.insert(snapshots_to_create, dataset .. "@" .. snapshot_name)

    local iterator = zfs.list.children(dataset)
    while true do
        local child = iterator()
        if child == nil then
            break
        end

        local include = true
        for _, excl in ipairs(excluded_datasets) do
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
populate_snapshots_to_create(dataset)

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
