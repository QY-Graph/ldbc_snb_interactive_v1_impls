package com.ldbc.driver.workloads.ldbc.snb.interactive;


import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorException;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

public class Query5EventStreamReader implements Iterator<Operation<?>> {
    private final Iterator<Object[]> csvRows;

    public Query5EventStreamReader(Iterator<Object[]> csvRows) {
        this.csvRows = csvRows;
    }

    @Override
    public boolean hasNext() {
        return csvRows.hasNext();
    }

    @Override
    public Operation<?> next() {
        Object[] rowAsObjects = csvRows.next();
        Operation<?> operation = new LdbcQuery5(
                (long) rowAsObjects[0],
                (Date) rowAsObjects[1],
                LdbcQuery5.DEFAULT_LIMIT
        );
        operation.setDependencyTimeAsMilli(0);
        return operation;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(String.format("%s does not support remove()", getClass().getSimpleName()));
    }

    public static class Query5Decoder implements CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> {
        /*
        Person|Date0
        7696581543848|1343952000
        */
        @Override
        public Object[] decodeEvent(CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) throws IOException {
            long personId;
            if (charSeeker.seek(mark, columnDelimiters)) {
                personId = charSeeker.extract(mark, extractors.long_()).longValue();
            } else {
                // if first column of next row contains nothing it means the file is finished
                return null;
            }

            Date date;
            if (charSeeker.seek(mark, columnDelimiters)) {
                date = new Date(charSeeker.extract(mark, extractors.long_()).longValue());
            } else {
                throw new GeneratorException("Error retrieving date");
            }

            return new Object[]{personId, date};
        }
    }
}
