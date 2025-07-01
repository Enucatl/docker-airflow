import os

from flask_appbuilder.const import AUTH_LDAP

AUTH_ROLE_ADMIN = "Admin"
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = AUTH_ROLE_ADMIN

AUTH_LDAP_BIND_USER = "uid=airflow,cn=users,cn=accounts,dc=home,dc=arpa"
AUTH_LDAP_BIND_PASSWORD = os.environ.get("AIRFLOW_LDAP_PASSWORD", "")
AUTH_LDAP_SERVER = os.environ.get("AUTH_LDAP_SERVER", "")

AUTH_LDAP_USE_TLS = False
AUTH_LDAP_TLS_CACERTFILE = os.environ.get("AUTH_LDAP_TLS_CACERTFILE", "")

# This is the base DN for your user searches.
# Combining your base_dn and additional_users_dn
AUTH_LDAP_SEARCH = "cn=users,cn=compat,dc=home,dc=arpa"

# The filter to find a user. The user-provided username will be substituted for `%(username)s`.
# Translated from: (&({username_attribute}={input})(objectClass=posixAccount))
# We use 'uid' because you specified it as the username attribute.
AUTH_LDAP_SEARCH_FILTER = "(objectClass=posixAccount)"

# The attribute that is used as the username for the Airflow user.
AUTH_LDAP_UID_FIELD = "uid"

# A mapping from LDAP attributes to User model fields.
# You wanted the display_name to be the uid. We map it to first_name.
AUTH_LDAP_FIRSTNAME_FIELD = "uid"
AUTH_LDAP_LASTNAME_FIELD = "uid"  # 'sn' is the standard for surname
AUTH_LDAP_EMAIL_FIELD = "mail"

# =================================================================
# Group-Based Role Mapping
# =================================================================
# Enable role mapping based on LDAP groups
AUTH_ROLES_SYNC_AT_LOGIN = True

# The base DN for your group searches.
# Combining your base_dn and additional_groups_dn
AUTH_LDAP_GROUP_SEARCH_BASE = "cn=groups,cn=compat,dc=home,dc=arpa"

# The filter to find the groups a user belongs to.
# The user's full DN will be substituted for `%(user_dn)s`, their username for `%(username)s`.
# Your filter `(&(memberUid={input})(objectClass=posixGroup))` uses the username (uid).
AUTH_LDAP_GROUP_SEARCH_FILTER = "(&(objectClass=posixGroup)(memberUid=%(username)s))"

# The attribute on the group object that contains the group's name.
AUTH_LDAP_GROUP_TITLE_ATTR = "cn"

# A mapping from LDAP Group DNs or group names (cn) to Airflow roles.
# The keys are the 'cn' from your LDAP groups.
# The values are the names of the Airflow roles (e.g., "Admin", "Op", "User", "Viewer").
AUTH_LDAP_ROLE_MAPPING = {
    "admins": "Admin",
    "ops": "Op",
    "users": "User",
}
AUTH_TYPE = AUTH_LDAP
