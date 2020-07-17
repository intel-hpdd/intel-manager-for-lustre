IML Task Runner
===============

Purpose
-------

This crate runs work assigned to Tasks.  It polls work from the database, and
parcels it out to the available workers.

Architecture
------------

A Task is specific type of work (purges from lpurge, or mirror extends from lamigo).
Work is generated and passed up to the database through iml-mailbox.

This crate processes Fids in the FidTaskQueue (chroma_core_fidtaskqueue) table.
It runs the associated actions as specified on the linked Task
(chroma_core_task) on an available worker.

The process is as follows:

    For each Worker (client mount) -> Find Tasks that worker can run
      For each Task found -> Find some fids and process them

