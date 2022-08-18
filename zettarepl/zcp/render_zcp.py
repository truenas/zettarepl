# -*- coding=utf-8 -*-
import logging
import typing

logger = logging.getLogger(__name__)

__all__ = ["render_zcp"]

zcp_program = r'''
snapshots_to_create = {}
function populate_snapshots_to_create(dataset, exclude_file)
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
populate_snapshots_to_create(dataset, excl_file)

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
'''

def render_vars(buffer: typing.IO[bytes], dataset: str, snapshot_name: str, excluded_datasets: typing.Iterable):
    buffer.write(f'dataset = "{dataset}"\n'.encode("utf-8"))
    buffer.write(f'snapshot_name = "{snapshot_name}"\n'.encode("utf-8"))
    buffer.write('excluded_datasets = {'.encode("utf-8"))

    for excluded_dataset in excluded_datasets:
        buffer.write(f'"{excluded_dataset}", '.encode("utf-8"))

    buffer.write('}\n'.encode("utf-8"))

def render_zcp(buffer: typing.IO[bytes], dataset: str, snapshot_name: str, excluded_datasets: typing.Iterable):
    render_vars(buffer, dataset, snapshot_name, excluded_datasets)

    buffer.write(zcp_program.encode("utf-8"))