"""
Shared enums used across microservices
"""
from enum import Enum


class UserRole(str, Enum):
    """User roles in the system"""
    MASTER_ADMIN = "master_admin"
    RESTAURANT_ADMIN = "restaurant_admin"
    CHEF = "chef"
    CUSTOMER = "customer"


class OrderStatus(str, Enum):
    """Order status lifecycle"""
    PENDING = "pending"
    CONFIRMED = "confirmed"
    PREPARING = "preparing"
    READY = "ready"
    SERVED = "served"
    CANCELLED = "cancelled"
    COMPLETED = "completed"


class TableStatus(str, Enum):
    """Table availability status"""
    AVAILABLE = "available"
    OCCUPIED = "occupied"
    RESERVED = "reserved"
    CLEANING = "cleaning"


class PricingPlan(str, Enum):
    """Restaurant pricing plans"""
    PER_TABLE = "per_table"
    BASIC = "basic"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"


class NotificationType(str, Enum):
    """Types of notifications"""
    ORDER_PLACED = "order_placed"
    ORDER_READY = "order_ready"
    ASSISTANCE_REQUESTED = "assistance_requested"
    TABLE_STATUS_CHANGE = "table_status_change"
    FEEDBACK_RECEIVED = "feedback_received"


class MenuItemCategory(str, Enum):
    """Menu item categories"""
    APPETIZER = "appetizer"
    MAIN_COURSE = "main_course"
    DESSERT = "dessert"
    BEVERAGE = "beverage"
    SIDE_DISH = "side_dish"
    SPECIAL = "special"


class SubscriptionStatus(str, Enum):
    """Restaurant subscription status"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"
    TRIAL = "trial"
    EXPIRED = "expired"


class PaymentStatus(str, Enum):
    """Payment status"""
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"


class OrderType(str, Enum):
    """Order channel type - table vs online"""
    TABLE = "table"      # In-restaurant table orders
    ONLINE = "online"    # Online delivery/pickup orders
