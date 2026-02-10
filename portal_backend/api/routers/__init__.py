from .auth_routes import router as auth_router
from .clients_routes import router as clients_router
from .guest_posts_routes import router as guest_posts_router
from .target_sites_routes import router as target_sites_router
from .admin_guest_posts_routes import router as admin_guest_posts_router
from .user_routes import router as user_router

__all__ = [
    "auth_router",
    "clients_router",
    "guest_posts_router",
    "target_sites_router",
    "admin_guest_posts_router",
    "user_router",
]
