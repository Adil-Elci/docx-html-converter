DROP TRIGGER IF EXISTS trg_clients_set_updated_at ON clients;
CREATE TRIGGER trg_clients_set_updated_at
BEFORE UPDATE ON clients
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_sites_set_updated_at ON sites;
CREATE TRIGGER trg_sites_set_updated_at
BEFORE UPDATE ON sites
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_site_credentials_set_updated_at ON site_credentials;
CREATE TRIGGER trg_site_credentials_set_updated_at
BEFORE UPDATE ON site_credentials
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_client_site_access_set_updated_at ON client_site_access;
CREATE TRIGGER trg_client_site_access_set_updated_at
BEFORE UPDATE ON client_site_access
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_submissions_set_updated_at ON submissions;
CREATE TRIGGER trg_submissions_set_updated_at
BEFORE UPDATE ON submissions
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_jobs_set_updated_at ON jobs;
CREATE TRIGGER trg_jobs_set_updated_at
BEFORE UPDATE ON jobs
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
