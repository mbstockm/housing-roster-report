import com.opencsv.CSVWriter
import com.opencsv.CSVWriterBuilder
import groovy.sql.Sql
import org.apache.commons.mail.Email
import org.apache.commons.mail.MultiPartEmail

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.ResultSet
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@GrabConfig(systemClassLoader = true)
@Grab(group='com.oracle.ojdbc', module='ojdbc8', version='19.3.0.0')
@Grab(group='com.opencsv', module='opencsv', version='5.8')
@Grab(group='org.apache.commons', module='commons-email', version='1.5')

def rundate = LocalDateTime.now()
def csv = Paths.get(System.properties.'user.dir',"housing-roster-report-${rundate.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))}.csv")
// When running script remember to set the following args on the command line or run configuration
def runterm = args[0]
def recipients = args[1]

Sql.withInstance(dbProps()) { sql ->
    housingRosterQuery(sql,runterm) { rs ->
        createCsv(csv,rs)
    }
    sendEmail(recipients,csv,rundate)
    Files.deleteIfExists(csv)
}

def housingRosterQuery(Sql sql, String runterm, Closure c) {
    sql.query("""
                  select spriden_id banner_id
                        ,spriden_last_name last_name
                        ,spriden_first_name first_name
                        ,nvl(spbpers_pref_first_name,spriden_first_name)  pref_name
                        ,substr(spriden_mi,1,1)||'.' mi
                        ,slrrasg_bldg_code bldg_code
                        ,slrrasg_room_number room_number
                        ,substr(slbrdef_phone_number,4,4) room_phone
                        ,sykpers.f_goremal(slrrasg_pidm,'UC') email
                        ,f_uc_getcohortcode(slrrasg_pidm,slrrasg_term_code) chrt
                    from slrrasg, spbpers p, spriden a, slbrdef b, stvascd
                   where slrrasg_term_code = ${runterm}
                     and b.slbrdef_term_code_eff = (select max(c.slbrdef_term_code_eff) 
                                                      from slbrdef c
                                                     where c.slbrdef_bldg_code = slrrasg_bldg_code
                                                       and c.slbrdef_room_number = slrrasg_room_number
                                                       and c.slbrdef_term_code_eff <= slrrasg_term_code)
                     and slrrasg_bldg_code = b.slbrdef_bldg_code 
                     and slrrasg_room_number = b.slbrdef_room_number 
                     and slrrasg_ascd_code = stvascd_code 
                     and stvascd_count_in_usage = 'Y' 
                     and f_uc_is_resdir(slrrasg_pidm,slrrasg_term_code) = 'N' 
                     and slrrasg_pidm = p.spbpers_pidm 
                     and p.spbpers_pidm = a.spriden_pidm 
                     and a.spriden_change_ind is NULL 
                  order by spriden_last_name, slrrasg_bldg_code
              """,
            c)
}

def createCsv(Path csv, ResultSet rs) {
    csv.withWriter { bw ->
        CSVWriter csvWriter = new CSVWriterBuilder(bw).build()
        csvWriter.writeAll(rs,true)
    }
}

def sendEmail(String recipients, Path csv, LocalDateTime rundate) {
    if (System.properties.'os.name'.toString().toUpperCase().contains('WINDOWS')) {
        println 'Skipped executing email commands while running in windows development environment. localmail host would not be setup.'
        return
    }
    Email email = new MultiPartEmail()
    email.setHostName('localhost')
    email.setSmtpPort(25)
    email.setFrom('noreply@utica.edu')
    email.setSubject("Housing Roster Report ${rundate.format(DateTimeFormatter.ofPattern( "yyyyMMddHHmmss"))}")
    email.setMsg('Report Attached.')
    email.addTo(recipients.split(','))
    email.attach(csv.toFile())
    email.send()
}

def dbProps() {
    def properties = new Properties();
    Paths.get(System.properties.'user.home','.credentials','bannerProduction.properties').withInputStream {
        properties.load(it)
    }
    return properties
}
