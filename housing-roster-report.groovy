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
                  with addr as (
                  select ad.spraddr_pidm pidm
                        ,ad.spraddr_atyp_code atyp
                        ,ad.spraddr_seqno seqno
                        ,ad.spraddr_street_line1 street1
                        ,ad.spraddr_street_line2 street2
                        ,ad.spraddr_street_line3 street3
                        ,ad.spraddr_street_line4 street4
                        ,ad.spraddr_city city
                        ,ad.spraddr_stat_code state_code
                        ,ad.spraddr_zip zip
                        ,ad.spraddr_cnty_code cnty
                        ,ad.spraddr_natn_code natn_code
                        ,row_number() over(partition by ad.spraddr_pidm order by decode(ad.spraddr_atyp_code,'MA',1,'PA',2,3) asc ) rown
                   from spraddr ad
                  where ad.spraddr_status_ind is null
                    and ad.spraddr_seqno = (select max(ad2.spraddr_seqno) from spraddr ad2
                                             where ad2.spraddr_pidm = ad.spraddr_pidm
                                               and ad2.spraddr_atyp_code = ad.spraddr_atyp_code
                                               and ad2.spraddr_status_ind is null
                                           )
                    and (ad.spraddr_from_date is null or ad.spraddr_from_date <= sysdate)
                    and (ad.spraddr_to_date is null or ad.spraddr_to_date >= sysdate)  
                    and ad.spraddr_atyp_code in ('MA','PA')             
                  )
                  select spriden_id banner_id
                        ,spriden_last_name last_name
                        ,spriden_first_name first_name
                        ,nvl(spbpers_pref_first_name,spriden_first_name)  pref_name
                        ,substr(spriden_mi,1,1)||'.' mi
                        ,slrrasg_bldg_code bldg_code
                        ,slrrasg_room_number room_number
                        ,substr(slbrdef_phone_number,4,4) room_phone
                        ,f_uc_getcohortcode(slrrasg_pidm,slrrasg_term_code) chrt
                        ,trim(to_char(spbpers_birth_date,'Month'))||' '||decode(substr(to_char(spbpers_birth_date,'DD'),1,1),'0',substr(to_char(spbpers_birth_date,'DD'),2,1),to_char(spbpers_birth_date,'DD'))||', '||to_char(spbpers_birth_date,'YYYY') birth_date
                        ,f_calculate_age(SYSDATE,spbpers_birth_date,spbpers_dead_date) age
                        ,f_uc_get_phone_number(ra.slrrasg_pidm,'MA') home_phone
                        ,f_uc_get_phone_number(ra.slrrasg_pidm,'CELL') cell_phone
                        ,addr.street1
                        ,addr.street2 
                        ,addr.street3
                        ,addr.city
                        ,addr.state_code
                        ,addr.zip
                        ,addr.natn_code
                        ,sykpers.f_goremal(slrrasg_pidm,'UC') email
                    from slrrasg ra
                         join spriden i 
                           on ra.slrrasg_pidm = i.spriden_pidm
                         left join spbpers p
                           on ra.slrrasg_pidm = p.spbpers_pidm
                         join slbrdef b
                           on ra.slrrasg_bldg_code = b.slbrdef_bldg_code
                              and ra.slrrasg_room_number = b.slbrdef_room_number
                              and b.slbrdef_term_code_eff = (select max(c.slbrdef_term_code_eff) 
                                                               from slbrdef c
                                                              where c.slbrdef_bldg_code = ra.slrrasg_bldg_code
                                                                and c.slbrdef_room_number = ra.slrrasg_room_number
                                                                and c.slbrdef_term_code_eff <= ra.slrrasg_term_code)
                         join stvascd
                           on ra.slrrasg_ascd_code = stvascd_code
                         join addr
                           on ra.slrrasg_pidm = addr.pidm
                              and addr.rown = 1
                   where ra.slrrasg_term_code = ${runterm}
                     and stvascd_count_in_usage = 'Y'
                     and i.spriden_change_ind is null 
                     and f_uc_is_resdir(ra.slrrasg_pidm,ra.slrrasg_term_code) = 'N' 
                  order by spriden_last_name, spriden_first_name, slrrasg_bldg_code
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
    if (!isJobsubServer()) {
        println 'Skipped executing email commands when not running script on jobsub server. Localmail host would not be setup.'
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

/**
 * Simple check to determine if script is running on jobsub sever.
 * @return
 */
def isJobsubServer() {
    if (System.properties.'os.name'.toString().toUpperCase().contains('LINUX')) {
        if (InetAddress.getLocalHost().getHostName().toString().toUpperCase().contains('JOBSUB')) {
            return true
        }
    }
    return false
}
