local function starts_with(str, start)
   return str:sub(1, #start) == start
end

args = ...
dataset = args["argv"][1]
snapshot_name = args["argv"][2]
exclude = {}
for i = 3, #args["argv"] do
    exclude[i - 2] = args["argv"][i]
end

zfs.sync.snapshot(dataset .. "@" .. snapshot_name)

iterator = zfs.list.children(dataset)
while true do
    child = iterator()
    if child == nil then
        break
    end

    include = true
    for i, excl in ipairs(exclude) do
        if child == excl or starts_with(child, excl .. "/") then
            include = false
            break
        end
    end
    if include then
        zfs.sync.snapshot(child .. "@" .. snapshot_name)
    end
end
