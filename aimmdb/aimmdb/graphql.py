from typing import Optional, Any

import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info
from strawberry.tools import create_type
from strawberry.permission import BasePermission

from fastapi import Depends, Security

from tiled.server.authentication import get_current_principal
from tiled.server.dependencies import get_root_tree


async def get_context(
    principal=Security(get_current_principal, scopes=["read:metadata"]),
    root_tree=Depends(get_root_tree),
):
    return {"principal": principal, "root_tree": root_tree}


class WritePermission(BasePermission):
    message = "User does not have write permission"

    async def has_permission(self, source: Any, info: Info, **kwargs) -> bool:
        try:
            principal = info.context["principal"]
            root = info.context["root_tree"]
            return root.access_policy.has_write_permission(principal)
        except Exception as e:
            print(f"Error while checking WritePermission: {e}")
            return False


class ReadPermission(BasePermission):
    message = "User does not have read permission"

    async def has_permission(self, source: Any, info: Info, **kwargs) -> bool:
        try:
            principal = info.context["principal"]
            root = info.context["root_tree"]
            return root.access_policy.has_read_permission(principal)
        except Exception as e:
            print(f"Error while checking ReadPermission: {e}")
            return False


@strawberry.field(permission_classes=[ReadPermission])
def hello(info: Info) -> str:
    principal = info.context["principal"]
    return f"Hello {principal}"


@strawberry.mutation(permission_classes=[WritePermission])
def record_message(message: str, info: Info) -> Optional[str]:
    root = info.context["root_tree"]
    db = root.db
    try:
        result = db.messages.insert_one({"message": message})
        return str(result.inserted_id)
    except RuntimeError as e:
        print(e)
        return None


GQLQuery = create_type("Query", [hello])
GQLMutation = create_type("Mutation", [record_message])

GQLSchema = strawberry.Schema(query=GQLQuery, mutation=GQLMutation)
GQLRouter = GraphQLRouter(GQLSchema, context_getter=get_context, graphiql=False)
