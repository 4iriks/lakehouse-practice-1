-- Snapshot ID from the recorded run.
SELECT * FROM lake.silver.evolution_demo FOR VERSION AS OF 662398616079528782;
ALTER TABLE lake.silver.evolution_demo EXECUTE rollback_to_snapshot(snapshot_id => 662398616079528782);
