from collections import defaultdict

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


class SimpleAccessPolicy:
    """
    A mapping of user names to global permissions

    >>> SimpleAccessPolicy({"alice": "rw", "bob": "r"}, provider="toy")
    """

    def __init__(self, access_lists, *, provider):
        self.access_lists = {}
        self.provider = provider
        for key, value in access_lists.items():
            if key == "public":
                key = SpecialUsers.public

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

    def permissions(self, principal):
        id = self.get_id(principal)
        if id is SpecialUsers.admin:
            return {READ, WRITE}
        else:
            return self.access_lists.get(id, set())

    def filter_results(self, tree, principal):
        if READ in self.permissions(principal):
            return tree.new_variation(principal=principal)
        else:
            return MapAdapter({})


class DatasetAccessPolicy:
    """
    A mapping of user names to per dataset permissions

    >>> DatasetAccessPolicy({"alice": {"foo" : "r", "bar" : "rw"}, "bob": {"foo" : "r", "bar" : "r"}}, provider="toy")
    """

    def __init__(self, access_lists, *, provider):
        self.access_lists = {}
        self.provider = provider

        # FIXME how to handle a normal user with the admin role?
        self.access_lists[SpecialUsers.admin] = defaultdict(lambda: {READ, WRITE})

        for principal_id, value in access_lists.items():
            if principal_id == "public":
                principal_id = SpecialUsers.public

            default_perm = value.pop("default", set())
            if default_perm:
                default_perm = str_to_permissions(default_perm)

            self.access_lists[principal_id] = defaultdict(
                lambda default_perm=default_perm: default_perm
            )

            for dset, perm in value.items():
                self.access_lists[principal_id][dset] = str_to_permissions(perm)

    def get_id(self, principal):
        # get the id (i.e. username) of this Principal for the associated authentication provider
        # passthough None and SpecialUsers

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

    def permissions(self, principal, dataset):
        principal_id = self.get_id(principal)
        principal_access_list = self.access_lists.get(principal_id, None)
        if principal_access_list is None:
            return set()
        else:
            return principal_access_list[dataset]

    def filter_results(self, tree, principal):
        principal_id = self.get_id(principal)
        principal_access_list = self.access_lists.get(principal_id, {})

        default = principal_access_list.default_factory()

        if READ in principal_access_list.default_factory():
            # FIXME default grants access to everything
            # should be able to exclude datasets
            return tree.new_variation(principal=principal)
        else:
            datasets = [k for k, v in principal_access_list.items() if READ in v]
            query = {"metadata.dataset": {"$in": datasets}}
            queries = tree.queries + [query]
            return tree.new_variation(principal=principal, queries=queries)
