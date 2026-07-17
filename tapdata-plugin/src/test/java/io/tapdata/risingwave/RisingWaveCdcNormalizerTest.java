package io.tapdata.risingwave;

import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RisingWaveCdcNormalizerTest {

    @Test
    void formsOneCompleteRowFromAvailableImages() {
        RisingWaveCdcNormalizer.Update update = normalize(
                ordersTable(),
                map("id", 1, "name", "before", "quantity", 42),
                map("id", 1, "name", "after"));

        assertEquals(map("id", 1, "name", "after", "quantity", 42), update.rowAfter);
        assertEquals(Collections.singletonMap("id", 1), update.oldFilter);
        assertFalse(update.keyChanged);
    }

    @Test
    void rejectsRelationalUpdatesWithoutACompletePostImage() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> normalize(ordersTable(), Collections.singletonMap("id", 1),
                        map("id", 1, "name", "after")));

        assertTrue(error.getMessage().contains("missing columns [quantity]"));
    }

    @Test
    void preservesNullAndTopLevelRemovalSemantics() {
        Map<String, Object> before = map("id", 1, "name", "before", "quantity", 42);
        RisingWaveCdcNormalizer.Update explicitNull = normalize(
                ordersTable(), before, map("id", 1, "name", "after", "quantity", null));
        assertNull(explicitNull.rowAfter.get("quantity"));

        TapUpdateRecordEvent removed = event(before, map("id", 1, "name", "after"));
        removed.removedFields(Collections.singletonList("quantity"));
        assertNull(RisingWaveCdcNormalizer.normalizeUpdate(ordersTable(), removed)
                .rowAfter.get("quantity"));
    }

    @Test
    void replaceAndAfterOnlyMongoDocumentsTreatAfterAsAuthoritative() {
        TapUpdateRecordEvent replace = event(
                map("id", 1, "name", "before", "quantity", 42),
                map("id", 1, "name", "replacement"));
        replace.setIsReplaceEvent(true);
        RisingWaveCdcNormalizer.Update replacement =
                RisingWaveCdcNormalizer.normalizeUpdate(ordersTable(), replace);
        assertEquals(map("id", 1, "name", "replacement", "quantity", null),
                replacement.rowAfter);

        TapTable mongo = new TapTable("documents")
                .add(new TapField("_id", "text").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("name", "text"))
                .add(new TapField("quantity", "integer"));
        RisingWaveCdcNormalizer.Update document = normalize(
                mongo, Collections.emptyMap(), map("_id", "a", "name", "current"));
        assertEquals(map("_id", "a", "name", "current", "quantity", null), document.rowAfter);
        assertEquals(Collections.singletonMap("_id", "a"), document.oldFilter);

        RisingWaveCdcNormalizer.Update withPreImage = normalize(
                mongo,
                map("_id", "a", "name", "before", "quantity", 42),
                map("_id", "a", "name", "current"));
        assertEquals(42, withPreImage.rowAfter.get("quantity"));
    }

    @Test
    void nestedRemovalRequiresTheCompleteParentColumn() {
        TapTable table = new TapTable("profiles")
                .add(new TapField("_id", "text").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("profile", "jsonb"));
        TapUpdateRecordEvent event = event(Collections.emptyMap(),
                Collections.singletonMap("_id", "a"));
        event.removedFields(Collections.singletonList("profile.name"));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> RisingWaveCdcNormalizer.normalizeUpdate(table, event));
        assertTrue(error.getMessage().contains("complete post-image for column profile"));
    }

    @Test
    void validatesOldIdentityForUpdatesAndDeletes() {
        assertThrows(IllegalArgumentException.class,
                () -> normalize(ordersTable(), Collections.emptyMap(),
                        map("id", 1, "name", "after", "quantity", 42)));

        TapTable mongo = new TapTable("documents")
                .add(new TapField("_id", "text").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("name", "text"));
        assertThrows(IllegalArgumentException.class,
                () -> normalize(mongo, Collections.singletonMap("name", "before"),
                        map("_id", "a", "name", "after")));
        assertThrows(IllegalArgumentException.class,
                () -> RisingWaveCdcNormalizer.deleteFilter(
                        ordersTable(), Collections.singletonMap("name", "before")));
        assertEquals(Collections.singletonMap("id", 1),
                RisingWaveCdcNormalizer.deleteFilter(ordersTable(), map("id", 1)));
    }

    @Test
    void supportsKeylessFiltersAndCompositePrimaryKeyChanges() {
        TapTable keyless = new TapTable("keyless")
                .add(new TapField("id", "integer"))
                .add(new TapField("name", "text"));
        Map<String, Object> before = map("id", 1, "name", "before");
        RisingWaveCdcNormalizer.Update keylessUpdate = normalize(
                keyless, before, Collections.singletonMap("name", "after"));
        assertEquals(before, keylessUpdate.oldFilter);
        assertFalse(keylessUpdate.keyChanged);
        assertThrows(IllegalArgumentException.class,
                () -> RisingWaveCdcNormalizer.deleteFilter(
                        keyless, Collections.singletonMap("id", 1)));

        TapTable composite = new TapTable("line_items")
                .add(new TapField("tenant", "integer").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(2))
                .add(new TapField("name", "text"));
        assertTrue(normalize(composite,
                map("tenant", 7, "id", 1, "name", "before"),
                map("tenant", 7, "id", 2, "name", "after")).keyChanged);
    }

    @Test
    void rejectsUnknownColumnsAndComparesNumericKeysByDatabaseValue() {
        assertThrows(IllegalArgumentException.class,
                () -> RisingWaveCdcNormalizer.validateColumns(
                        ordersTable(), map("id", 1, "new_column", "value")));

        TapTable table = new TapTable("numeric_keys")
                .add(new TapField("id", "numeric").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("name", "text"));
        RisingWaveCdcNormalizer.Update update = normalize(
                table,
                map("id", new BigDecimal("1.0"), "name", "before"),
                map("id", new BigDecimal("1.00"), "name", "after"));
        assertFalse(update.keyChanged);

        TapTable varchar = new TapTable("varchar_keys")
                .add(new TapField("id", "varchar").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("name", "text"));
        assertTrue(normalize(varchar,
                map("id", new BigDecimal("1.0"), "name", "before"),
                map("id", new BigDecimal("1.00"), "name", "after")).keyChanged);
    }

    @Test
    void usesTapdataDefaultPrimaryKeysWhenFieldsAreNotMarked() {
        TapTable table = new TapTable("documents")
                .add(new TapField("_id", "text"))
                .add(new TapField("name", "text"))
                .defaultPrimaryKeys("_id");

        assertEquals(Collections.singletonList("_id"),
                RisingWaveCdcNormalizer.primaryKeys(table));
        assertEquals(Collections.singletonMap("_id", "a"),
                RisingWaveCdcNormalizer.deleteFilter(table, map("_id", "a")));
    }

    private static RisingWaveCdcNormalizer.Update normalize(
            TapTable table, Map<String, Object> before, Map<String, Object> after) {
        return RisingWaveCdcNormalizer.normalizeUpdate(table, event(before, after));
    }

    private static TapUpdateRecordEvent event(
            Map<String, Object> before, Map<String, Object> after) {
        return TapUpdateRecordEvent.create().before(before).after(after);
    }

    private static TapTable ordersTable() {
        return new TapTable("orders")
                .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("name", "text"))
                .add(new TapField("quantity", "integer"));
    }

    private static Map<String, Object> map(Object... values) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) {
            map.put((String) values[index], values[index + 1]);
        }
        return map;
    }
}
