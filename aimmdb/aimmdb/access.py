from tiled.utils import SpecialUsers, import_object
from tiled.adapters.mapping import MapAdapter


class AIMMAccessPolicy:
    READ = object()  # sentinel
    READWRITE = object()  # sentinel

    def __init__(self, access_lists, *, provider):
        self.access_lists = {}
        self.provider = provider
        for key, value in access_lists.items():
            if isinstance(value, str):
                value = import_object(value)
            if value not in (self.READ, self.READWRITE):
                raise KeyError(
                    f"AIMMAccessPolicy: value {value} is not AIMMAccessPolicy.READ or AIMMAcccessPolicy.READWRITE"
                )
            self.access_lists[key] = value

    def get_id(self, principal):
        # Get the id (i.e. username) of this Principal for the
        # associated authentication provider.
        for identity in principal.identities:
            if identity.provider == self.provider:
                return identity.id
        else:
            raise ValueError(
                f"Principcal {principal} has no identity from provider {self.provider}. "
                f"Its identities are: {principal.identities}"
            )

    def has_read_permission(self, principal):
        id = self.get_id(principal)
        return (principal is SpecialUsers.admin) or (id in self.access_lists.keys())

    def has_write_permission(self, principal):
        id = self.get_id(principal)
        permission = self.access_lists.get(id, None)
        return (principal is SpecialUsers.admin) or (permission is self.READWRITE)

    def filter_results(self, tree, principal):
        if self.has_read_permission(principal):
            return tree
        else:
            return MapAdapter({})
