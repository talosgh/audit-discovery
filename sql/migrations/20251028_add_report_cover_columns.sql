ALTER TABLE report_jobs
    ADD COLUMN IF NOT EXISTS cover_building_owner TEXT,
    ADD COLUMN IF NOT EXISTS cover_street TEXT,
    ADD COLUMN IF NOT EXISTS cover_city TEXT,
    ADD COLUMN IF NOT EXISTS cover_state TEXT,
    ADD COLUMN IF NOT EXISTS cover_zip TEXT,
    ADD COLUMN IF NOT EXISTS cover_contact_name TEXT,
    ADD COLUMN IF NOT EXISTS cover_contact_email TEXT;

