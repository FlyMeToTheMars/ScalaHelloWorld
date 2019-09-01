import java.util.Arrays;
import java.util.List;

public class timeanalog {

    public static void main(String[] args) {

        String tz = "_d5c";
        String table = "east_five_c";

        String index = "a.";

//        String distance = index+"distance"+tz;
//        String runTimeSecond = index+"runTimeSecond"+tz;
//        String starttime = index+"starttime"+tz;
//        String endtime = index+"endtime"+tz;
//        String startLng = index+"startLng"+tz;
//        String endLng = index+"endLng"+tz;
//        String startlat = index+"startlat"+tz;
//        String endlat = index+"endlat"+tz;


        /*System.out.println("spark.sql(");
        System.out.println("    s\" SELECT imei,date_format(createTime, 'yyyy-MM-dd') atDay,SUM(distance) OVER(PARTITION BY s.imei) " + distance + ",\" +");
        System.out.println("    s\" SUM(runTimeSecond) OVER(PARTITION BY s.imei) as "+ runTimeSecond +",\" +");
        System.out.println("    s\" first_value(s.startTime) over(PARTITION BY s.imei ORDER BY s.startTime ) as "+starttime+",\" +");
        System.out.println("    s\" first_value(s.endTime) OVER(PARTITION BY s.imei ORDER BY s.endTime desc ) as "+endtime+",\" +");
        System.out.println("    s\" first_value(s.startLng) over(PARTITION BY s.imei ORDER BY s.startTime ) as "+startLng+",\" +");
        System.out.println("    s\" first_value(s.endLng) OVER(PARTITION BY s.imei ORDER BY s.endTime desc ) as "+endLng+",\" +");
        System.out.println("    s\" first_value(s.startlat) OVER(PARTITION BY s.imei ORDER BY s.startTime) as "+startlat+",\" +");
        System.out.println("    s\" first_value(s.endlat) over(PARTITION BY s.imei ORDER BY s.endtime desc) as "+endlat +" FROM mileageOri s   \"  + ");
        System.out.println("    s\" where createTime >= '$preday 11' and createTime < '$atday 11'\"");
        System.out.println("    ).createOrReplaceTempView(\""+table +"\")");*/

//        System.out.println(distance+","+runTimeSecond+","+starttime+","+endtime+","+startLng+","+endLng+","+startlat+","+endlat);

        List<String> list = Arrays.asList("_d6","_d6b","_d7","_d8","_d9","_d9b","_d10","_d10b","_d11","_d12","_d13");

        List<String> cha = Arrays.asList("a.","b.","c.","d.","e.","f.","g.","h.","i.","j.");

        for(int i = 0;i <= 9;i++){
            tz = list.get(i);
            String indexx = cha.get(i);

            String distance = indexx+"distance"+tz;
            String runTimeSecond = indexx+"runTimeSecond"+tz;
            String starttime = indexx+"starttime"+tz;
            String endtime = indexx+"endtime"+tz;
            String startLng = indexx+"startLng"+tz;
            String endLng = indexx+"endLng"+tz;
            String startlat = indexx+"startlat"+tz;
            String endlat = indexx+"endlat"+tz;

            System.out.println(distance+","+runTimeSecond+","+starttime+","+endtime+","+startLng+","+endLng+","+startlat+","+endlat);

        }
    }
}
