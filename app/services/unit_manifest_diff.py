from collections.abc import Iterator

from app.dto.enum import FieldKey, FieldType, UnitSchemaWarningKind
from app.schemas.pydantic.unit import UnitSchemaWarningRead
from app.schemas.pydantic.unit_manifest import (
    FieldDeclaration,
    FieldDeclarations,
    UnitDefinition,
)


class UnitManifestDiff:
    CONTRACT_KEYS = (
        FieldKey.OPTIONAL,
        FieldKey.NULLABLE,
        FieldKey.EXTRA,
        FieldKey.CODEC,
        FieldKey.QOS,
        FieldKey.ROUTING,
        FieldKey.UNIT,
        FieldKey.MIN,
        FieldKey.MAX,
        FieldKey.MIN_ITEMS,
        FieldKey.MAX_ITEMS,
        FieldKey.MAX_BYTES,
        FieldKey.MAX_LENGTH,
        FieldKey.FORMAT,
        FieldKey.ENUM,
        FieldKey.DEFAULT,
    )

    @classmethod
    def compare(
        cls,
        before: UnitDefinition,
        after: UnitDefinition,
        before_env: FieldDeclarations,
        after_env: FieldDeclarations,
    ) -> Iterator[UnitSchemaWarningRead]:
        for section, old_fields, new_fields in (
            (FieldKey.INPUT, before.input, after.input),
            (FieldKey.OUTPUT, before.output, after.output),
            (FieldKey.ENV, before_env, after_env),
        ):
            yield from cls.compare_fields(
                old_fields,
                new_fields,
                (section.value,),
                warn_on_addition=False,
            )
        for name in sorted(after_env.keys() - before_env.keys()):
            field = after_env[name]
            if (
                not field.get(FieldKey.OPTIONAL)
                and FieldKey.DEFAULT not in field
            ):
                yield UnitSchemaWarningRead(
                    kind=UnitSchemaWarningKind.ADDED,
                    path=(FieldKey.ENV.value, name),
                )

    @classmethod
    def compare_fields(
        cls,
        before: FieldDeclarations,
        after: FieldDeclarations,
        path: tuple[str, ...],
        *,
        warn_on_addition: bool = True,
    ) -> Iterator[UnitSchemaWarningRead]:
        for name in sorted(before.keys() | after.keys()):
            field_path = (*path, name)
            if name not in after:
                yield UnitSchemaWarningRead(
                    kind=UnitSchemaWarningKind.REMOVED, path=field_path
                )
            elif name not in before:
                if warn_on_addition:
                    yield UnitSchemaWarningRead(
                        kind=UnitSchemaWarningKind.ADDED, path=field_path
                    )
            else:
                yield from cls.compare_field(
                    before[name], after[name], field_path
                )

    @classmethod
    def compare_field(
        cls,
        before: FieldDeclaration,
        after: FieldDeclaration,
        path: tuple[str, ...],
    ) -> Iterator[UnitSchemaWarningRead]:
        before_type = FieldType(before[FieldKey.TYPE])
        after_type = FieldType(after[FieldKey.TYPE])
        if before_type != after_type:
            yield UnitSchemaWarningRead(
                kind=UnitSchemaWarningKind.TYPE_CHANGED,
                path=(*path, FieldKey.TYPE.value),
                before_type=before_type,
                after_type=after_type,
            )
            return

        for key in cls.CONTRACT_KEYS:
            default = 0 if key == FieldKey.MIN_ITEMS else None
            previous = before.get(key, default)
            changed = after.get(key, default)
            if (
                key == FieldKey.ENUM
                and previous is not None
                and changed is not None
            ):
                previous, changed = set(previous), set(changed)
            default_added_or_removed = key == FieldKey.DEFAULT and (
                (key in before) != (key in after)
            )
            if previous != changed or default_added_or_removed:
                yield UnitSchemaWarningRead(
                    kind=UnitSchemaWarningKind.CHANGED, path=(*path, key.value)
                )

        if before_type == FieldType.ARRAY:
            yield from cls.compare_field(
                before[FieldKey.ITEMS],
                after[FieldKey.ITEMS],
                (*path, FieldKey.ITEMS.value),
            )
        elif before_type == FieldType.OBJECT:
            yield from cls.compare_fields(
                before.get(FieldKey.FIELDS, {}),
                after.get(FieldKey.FIELDS, {}),
                (*path, FieldKey.FIELDS.value),
            )
            yield from cls.compare_variants(
                before.get(FieldKey.VARIANTS, {}),
                after.get(FieldKey.VARIANTS, {}),
                (*path, FieldKey.VARIANTS.value),
            )

    @classmethod
    def compare_variants(
        cls,
        before: dict[str, FieldDeclarations],
        after: dict[str, FieldDeclarations],
        path: tuple[str, ...],
    ) -> Iterator[UnitSchemaWarningRead]:
        for name in sorted(before.keys() | after.keys()):
            variant_path = (*path, name)
            if name not in after:
                yield UnitSchemaWarningRead(
                    kind=UnitSchemaWarningKind.REMOVED, path=variant_path
                )
            elif name not in before:
                yield UnitSchemaWarningRead(
                    kind=UnitSchemaWarningKind.ADDED, path=variant_path
                )
            else:
                yield from cls.compare_fields(
                    before[name], after[name], variant_path
                )
