from flask_appbuilder.const import AUTH_REMOTE_USER

from airflow.providers.fab.auth_manager.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)

AUTH_TYPE = AUTH_REMOTE_USER
AUTH_REMOTE_USER_ENV_VAR = "HTTP_REMOTE_USER"
AUTH_ROLE_ADMIN = "Admin"
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = AUTH_ROLE_ADMIN


class RemoteUserFixFabAirflowSecurityManagerOverride(FabAirflowSecurityManagerOverride):
    def __init__(self, appbuilder):
        self.auth_remote_user_env_var = AUTH_REMOTE_USER_ENV_VAR
        super().__init__(appbuilder)

    def auth_user_remote_user(self, username):
        """
        REMOTE_USER user Authentication

        :param username: user's username for remote auth
        :type self: User model
        """
        user = self.find_user(username=username)

        # User does not exist, create one if auto user registration.
        if user is None and self.auth_user_registration:
            user = self.add_user(
                # All we have is REMOTE_USER, so we set
                # the other fields to blank.
                username=username,
                first_name=username,
                last_name="-",
                email=username + "@email.notfound",
                role=self.find_role(self.auth_user_registration_role),
            )

        # If user does not exist on the DB and not auto user registration,
        # or user is inactive, go away.
        elif user is None or (not user.is_active):
            return None

        self.update_user_auth_stat(user)
        return user


SECURITY_MANAGER_CLASS = RemoteUserFixFabAirflowSecurityManagerOverride
