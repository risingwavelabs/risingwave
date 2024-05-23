package com.risingwave.connector.deserializer;

import com.risingwave.connector.api.sink.CloseableIterable;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.java.binding.StreamChunk;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class ArrowIter implements Iterator<SinkRow>, AutoCloseable {
    private final FieldVector ops;
    private final VectorSchemaRoot root;
    private int currentIndex;

    public ArrowIter(List<Long> arrowArrayPointers, List<Long> arrowSchemaPointers) {
        BufferAllocator allocator = new RootAllocator();
        ArrayList<FieldVector> vectors = new ArrayList<>();
        for (int i = 0; i < arrowArrayPointers.size(); i++) {
            long a = arrowArrayPointers.get(i);
            long b = arrowSchemaPointers.get(i);
            ArrowSchema arrowSchema = ArrowSchema.wrap(a);
            ArrowArray arrowArray = ArrowArray.wrap(b);
            FieldVector vector =
                    org.apache.arrow.c.Data.importVector(allocator, arrowArray, arrowSchema, null);
            vectors.add(vector);
            // System.out.println("Rust allocated array: " + vector);
        }
        var ops = vectors.remove(0);
        VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        this.root = root;
        this.currentIndex = 0;
        this.ops = ops;
    }

    @Override
    public boolean hasNext() {
        return currentIndex < root.getRowCount();
    }

    @Override
    public ArrowRow next() {
        List<Object> rows = new ArrayList<>();
        for (FieldVector vector : root.getFieldVectors()) {
            rows.add(vector.getObject(currentIndex));
        }
        short opValue = (short) ops.getObject(currentIndex);
        currentIndex++;
        ArrowRow arrowRow = new ArrowRow(rows, opValue);
        return arrowRow;
    }

    @Override
    public void close() {
        root.close();
    }
}

class ArrowIterable implements CloseableIterable<SinkRow> {
    private final StreamChunk chunk;
    private ArrowIter iter;

    public ArrowIterable(StreamChunk chunk) {
        this.chunk = chunk;
        this.iter = null;
    }

    void clearIter() {
        if (this.iter != null) {
            this.iter.close();
        }
    }

    @Override
    public void close() throws Exception {
        clearIter();
        chunk.close();
    }

    @Override
    public Iterator<SinkRow> iterator() {
        clearIter();
        this.iter = new ArrowIter(chunk.getArrowArrayPointers(), chunk.getArrowSchemaPointers());
        return this.iter;
    }
}
