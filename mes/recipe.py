"""Recipe Management — ISA-88 Batch recipe model with version control and audit."""
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class RecipeParameter:
    param_name: str
    param_value: float
    unit: str = ""
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    def is_within_spec(self, value: float) -> bool:
        if self.min_value is not None and value < self.min_value:
            return False
        if self.max_value is not None and value > self.max_value:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "param_name": self.param_name,
            "param_value": self.param_value,
            "unit": self.unit,
            "min_value": self.min_value,
            "max_value": self.max_value,
        }


@dataclass
class AuditEntry:
    action: str
    old_values: dict = field(default_factory=dict)
    new_values: dict = field(default_factory=dict)
    changed_by: str = "system"
    timestamp: float = field(default_factory=time.time)


@dataclass
class Recipe:
    recipe_code: str
    product_code: str
    version: int = 1
    status: str = "draft"  # draft, active, deprecated
    description: str = ""
    parameters: list = field(default_factory=list)
    audit_log: list = field(default_factory=list)
    created_by: str = "system"
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "recipe_code": self.recipe_code,
            "product_code": self.product_code,
            "version": self.version,
            "status": self.status,
            "description": self.description,
            "parameters": [p.to_dict() for p in self.parameters],
            "created_by": self.created_by,
            "created_at": self.created_at,
        }


class RecipeService:
    """In-memory recipe management with version control."""

    def __init__(self):
        self._recipes: dict[str, Recipe] = {}
        self._versions: dict[str, list[Recipe]] = {}  # recipe_code -> [versions]
        self._init_sample_recipes()

    def _init_sample_recipes(self):
        recipes = [
            ("RCP-001", "PRODUCT_A", "Standart uretim recetesi", [
                RecipeParameter("temperature", 55.0, "°C", 45.0, 65.0),
                RecipeParameter("pressure", 6.0, "bar", 4.0, 8.0),
                RecipeParameter("speed", 1500, "rpm", 1000, 2000),
                RecipeParameter("current_limit", 12.0, "A", 0, 15.0),
            ]),
            ("RCP-002", "PRODUCT_B", "Yuksek hassasiyet recetesi", [
                RecipeParameter("temperature", 48.0, "°C", 40.0, 55.0),
                RecipeParameter("pressure", 5.0, "bar", 3.5, 6.5),
                RecipeParameter("speed", 1200, "rpm", 800, 1600),
                RecipeParameter("current_limit", 10.0, "A", 0, 13.0),
            ]),
            ("RCP-003", "PRODUCT_C", "Hizli uretim recetesi", [
                RecipeParameter("temperature", 62.0, "°C", 50.0, 70.0),
                RecipeParameter("pressure", 7.5, "bar", 5.0, 9.0),
                RecipeParameter("speed", 1800, "rpm", 1200, 2200),
                RecipeParameter("current_limit", 14.0, "A", 0, 16.0),
            ]),
        ]
        for code, product, desc, params in recipes:
            r = Recipe(recipe_code=code, product_code=product, description=desc,
                       parameters=params, status="active")
            self._recipes[code] = r
            self._versions[code] = [r]

    def get_all(self, status: str = None) -> list[Recipe]:
        recipes = list(self._recipes.values())
        if status:
            recipes = [r for r in recipes if r.status == status]
        return recipes

    def get(self, recipe_code: str) -> Optional[Recipe]:
        return self._recipes.get(recipe_code)

    def create(self, recipe_code: str, product_code: str, description: str,
               parameters: list[dict], created_by: str = "system") -> Recipe:
        params = [RecipeParameter(**p) for p in parameters]
        recipe = Recipe(
            recipe_code=recipe_code,
            product_code=product_code,
            description=description,
            parameters=params,
            created_by=created_by,
        )
        recipe.audit_log.append(AuditEntry("create", changed_by=created_by))
        self._recipes[recipe_code] = recipe
        self._versions[recipe_code] = [recipe]
        log.info(f"Recipe created: {recipe_code}")
        return recipe

    def update(self, recipe_code: str, parameters: list[dict],
               changed_by: str = "system") -> Optional[Recipe]:
        old = self._recipes.get(recipe_code)
        if not old:
            return None

        new_params = [RecipeParameter(**p) for p in parameters]
        new_recipe = Recipe(
            recipe_code=recipe_code,
            product_code=old.product_code,
            version=old.version + 1,
            description=old.description,
            parameters=new_params,
            status="draft",
            created_by=changed_by,
        )
        old_vals = {p.param_name: p.param_value for p in old.parameters}
        new_vals = {p.param_name: p.param_value for p in new_params}
        new_recipe.audit_log = list(old.audit_log)
        new_recipe.audit_log.append(AuditEntry("update", old_vals, new_vals, changed_by))

        self._recipes[recipe_code] = new_recipe
        if recipe_code not in self._versions:
            self._versions[recipe_code] = []
        self._versions[recipe_code].append(new_recipe)
        log.info(f"Recipe updated: {recipe_code} v{new_recipe.version}")
        return new_recipe

    def activate(self, recipe_code: str, changed_by: str = "system") -> bool:
        recipe = self._recipes.get(recipe_code)
        if not recipe:
            return False
        recipe.status = "active"
        recipe.audit_log.append(AuditEntry("activate", changed_by=changed_by))
        return True

    def deprecate(self, recipe_code: str, changed_by: str = "system") -> bool:
        recipe = self._recipes.get(recipe_code)
        if not recipe:
            return False
        recipe.status = "deprecated"
        recipe.audit_log.append(AuditEntry("deprecate", changed_by=changed_by))
        return True

    def get_versions(self, recipe_code: str) -> list[dict]:
        versions = self._versions.get(recipe_code, [])
        return [v.to_dict() for v in versions]

    def get_audit_trail(self, recipe_code: str) -> list[dict]:
        recipe = self._recipes.get(recipe_code)
        if not recipe:
            return []
        return [
            {
                "action": a.action,
                "old_values": a.old_values,
                "new_values": a.new_values,
                "changed_by": a.changed_by,
                "timestamp": a.timestamp,
            }
            for a in recipe.audit_log
        ]

    def apply_to_machine(self, recipe_code: str, machine_code: str) -> dict:
        """Simulate writing recipe parameters to PLC/OPC UA."""
        recipe = self._recipes.get(recipe_code)
        if not recipe:
            return {"ok": False, "error": "Recipe not found"}
        params_written = {p.param_name: p.param_value for p in recipe.parameters}
        log.info(f"Recipe {recipe_code} applied to {machine_code}: {params_written}")
        return {"ok": True, "machine_code": machine_code, "parameters_written": params_written}
