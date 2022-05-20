from fastapi import HTTPException

from tiled.adapters.mapping import MapAdapter
from tiled.utils import SpecialUsers, import_object

READ = object()  # sentinel
WRITE = object()  # sentinel


def str_to_permissions(permission_string: str):
    if permission_string == "r":
        return {READ}
    elif permission_string == "rw":
        return {READ, WRITE}
    else:
        raise ValueError(
            f"permission string {permission_string} must be either 'r' or 'rw'"
        )


def require_write_permission(method):
    def inner(self, *args, **kwargs):
        if WRITE not in self.permissions:
            raise HTTPException(
                status_code=403, detail="principal does not have write permission"
            )
        else:
            return method(self, *args, **kwargs)

    return inner


class AIMMAccessPolicy:
    def __init__(self, access_lists, *, provider):
        self.access_lists = {}
        self.provider = provider
        for key, value in access_lists.items():
            self.access_lists[key] = str_to_permissions(value)

    def get_id(self, principal):
        # Get the id (i.e. username) of this Principal for the
        # associated authentication provider.

        if principal is None:
            return None
        elif isinstance(principal, SpecialUsers):
            return principal
        else:
            for identity in principal.identities:
                if identity.provider == self.provider:
                    return identity.id
            else:
                raise ValueError(
                    f"Principcal {principal} has no identity from provider {self.provider}. "
                    f"Its identities are: {principal.identities}"
                )

    def permissions(self, tree, principal):
        id = self.get_id(principal)
        if id is SpecialUsers.admin:
            return {READ, WRITE}
        else:
            return self.access_lists.get(id, set())

    def filter_results(self, tree, principal):
        if READ in self.permissions(tree, principal):
            return tree.new_variation(principal=principal)
        else:
            return MapAdapter({})
