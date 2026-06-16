-- 0007_device_delivered — track the per-device DELIVERED watermark so cursorAck
-- can be bounded to what a device actually read (not the inbox's global max).
--
-- AUDIT FIX: cursorAck previously clamped throughSeq to the inbox high-water, so
-- a device could "consume" seqs it never received. readAfterCursor now advances
-- last_delivered, and cursorAck clamps to it — only a registered device that was
-- actually delivered those seqs may advance its cursor.

ALTER TABLE device_cursors
  ADD COLUMN IF NOT EXISTS last_delivered bigint NOT NULL DEFAULT 0;
