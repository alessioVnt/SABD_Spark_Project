package utilities;

import com.skedgo.converter.TimezoneMapper;
import org.joda.time.LocalDateTime;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TimeZoneConverter {

    public static int getTimeZoneOffset(long latitude, long longitude){

        String timezone = TimezoneMapper.latLngToTimezoneString(latitude,longitude);

        ZoneId here = ZoneId.of(timezone);
        ZonedDateTime hereAndNow = Instant.now().atZone(here);

        String offset = hereAndNow.getOffset().toString();

        if(offset.equals("Z")){
            return 0;
        }
        if(offset.startsWith("+")){
            return Integer.parseInt(offset.substring(1,3));
        }
        else{
            return -Integer.parseInt(offset.substring(1,3));
        }

    }

    public static LocalDateTime convertTimeZone(LocalDateTime dateToConv, long latitude, long longitude){

        LocalDateTime convertedTimeZone = dateToConv.plusHours(getTimeZoneOffset(latitude,longitude));

        return convertedTimeZone;
    }

}
