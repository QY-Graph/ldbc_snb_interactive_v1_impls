package com.ldbc.impls.workloads.ldbc.snb.util;

import org.openrdf.query.Binding;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class SparqlConverter extends Converter {

    /**
     * Converts epoch seconds to SPARQL timestamps.
     * @param timestamp
     * @return
     */
    @Override
    public String convertDate(long timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return "'"+sdf.format(new Date(timestamp))+"'::timestamp";
    }

    public long convertTimestampToEpoch(Binding binding) throws ParseException {
        final String timestamp = binding.getValue().stringValue();
        final SimpleDateFormat sdf = new SimpleDateFormat(DATAGEN_FORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf.parse(timestamp).toInstant().toEpochMilli();
    }

    public String convertString(String value) {
        return "'" + value + "'";
    }

    public String convertStringList(List<String> values) {
        return values
                .stream()
                .map(v -> "'" + v + "'")
                .collect( Collectors.joining( "," ) );
    }

}
