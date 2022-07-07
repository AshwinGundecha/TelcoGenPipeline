import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.joda.time.DateTime;


import java.util.Date;

/**
 * A class used for parsing JSON web server events
 * Annotated with @DefaultSchema to the allow the use of Beam Schema and <Row> object
 */
@DefaultSchema(JavaFieldSchema.class)
public class CommonLog {
    String SystemIdentity;
    String RecordType;
    int TimeType;
    String ServiceType;
    int EndType;
    String OutgoingTrunk;
    int Transfer;
    int CallingIMSI;
    int CalledIMSI;
    int MSRN;
    int FileNum;
    String SwitchNum;
    DateTime Date;
    DateTime Time;
    DateTime DateTime;
    int CalledNum;
    int CallingNum;
    int CallPeriod;


    @SchemaCreate
    public CommonLog(String systemIdentity, String recordType, int timeType, String serviceType, int endType, String outgoingTrunk, int transfer, int callingIMSI, int calledIMSI, int MSRN, int fileNum, String switchNum, org.joda.time.DateTime date, org.joda.time.DateTime time, org.joda.time.DateTime dateTime, int calledNum, int callingNum, int callPeriod) {
        this.SystemIdentity = systemIdentity;
        this.RecordType = recordType;
        this.TimeType = timeType;
        this.ServiceType = serviceType;
        this.EndType = endType;
        this.OutgoingTrunk = outgoingTrunk;
        this.Transfer = transfer;
        this.CallingIMSI = callingIMSI;
        this.CalledIMSI = calledIMSI;
        this.MSRN = MSRN;
        this.FileNum = fileNum;
        this.SwitchNum = switchNum;
        this.Date = date;
        this.Time = time;
        this.DateTime = dateTime;
        this.CalledNum = calledNum;
        this.CallingNum = callingNum;
        this.CallPeriod = callPeriod;
    }
}