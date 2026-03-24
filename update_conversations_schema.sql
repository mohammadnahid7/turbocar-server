-- ============================================================================
-- Migration: Remove denormalized buyer/seller info from conversations table
--
-- REASON: buyer_name, buyer_avatar_url, seller_name, seller_avatar_url were
-- hardcoded at conversation creation time. They become stale when users update
-- their profiles. Instead, we now JOIN the users table at query time.
--
-- HOW TO RUN: Execute this script in the Supabase SQL Editor or via psql.
-- ============================================================================

ALTER TABLE conversations
  DROP COLUMN IF EXISTS buyer_name,
  DROP COLUMN IF EXISTS buyer_avatar_url,
  DROP COLUMN IF EXISTS seller_name,
  DROP COLUMN IF EXISTS seller_avatar_url;
