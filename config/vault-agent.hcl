pid_file = "/home/vault/pidfile"

vault {
  address = "${VAULT_ADDR}"
}

auto_auth {
  method "approle" {
    mount_path = "auth/approle"
    config = {
      role_id_file_path   = "/vault/config/role_id"
      secret_id_file_path = "/vault/config/secret_id"
      remove_secret_id_file_after_read = false
    }
  }

  sink "file" {
    config = {
      path = "/vault/secrets/token"
    }
  }
}

# This template will automatically "pull" a new SecretID and write it
# over the old one, creating a perpetual rotation loop.
template {
  # This is the Vault API endpoint the agent will call using its valid token.
  # It fetches a new SecretID for the 'airflow' role.
  source      = " {{ with secret \"auth/approle/role/airflow/secret-id\" }} {{ .Data.secret_id }} {{ end }} "
  
  # This is the same file the agent reads the SecretID from.
  # It will overwrite the old ID with the new one.
  destination = "/vault/config/secret_id"
}
