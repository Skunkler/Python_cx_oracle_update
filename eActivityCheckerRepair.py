#Script was written By Warren Kunkler in support of Eactivity Health Check processing on 5/07/2021
#helper classes that hold plsql statements to run based on logic called within the update eactivities script
import os, cx_Oracle, datetime


class Crk(object):
    

    def __init__(self, user, pswd, dns):
        self.conn = cx_Oracle.connect(user, pswd, dns)
        self._Messages = ""
        self.getRec = """
        DECLARE
            v_number_key        apd_adr.number_key%TYPE;
            v_site_element_key  apd_adr.site_element_key%TYPE;
            
            
        BEGIN
           
        
            FOR insertRec in (
                select number_key
                from crystal.v_eactivities
                where number_key not in (select number_key
                                from eactivities@cagis.pub1) and number_key in (select apd.number_key from scsiprod.apd_adr apd inner join scsiprod.adr_base base on apd.SITE_ELEMENT_KEY = base.ELEMENT_KEY))
                                
            
       
        LOOP
           
            select max(number_key), max(site_element_key)
            into v_number_key, v_site_element_key
            from scsiprod.apd_adr
            where number_key = insertRec.number_key 
            group by number_key;
            CAGIS_EACTIVITIES.CREATEGISAPD_FEATURES(v_site_element_key, v_number_key);
            DBMS_OUTPUT.PUT_LINE(v_number_key || ', ' || v_site_element_key);
           
        END LOOP;
        
       
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('No data found for: ' || v_number_key || ', ' || v_site_element_key);
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE(SQLCODE || ' ' || SQLERRM);
    
    END;
    """
        self.UpdateStatus = """
        DECLARE
            VIN_NUMBER_KEY     CHAR (10);
            VIN_DATA_STATUS    CHAR (8);
            VIN_STATUS_CLASS   VARCHAR2 (4);
            VIN_SUB_TYPE       CHAR (8);
            VIN_STATUS_TABLE   VARCHAR2 (8);
              
        BEGIN
            FOR updateRec
                IN (SELECT e.number_key
        FROM cagis.eactcincopenpmce@l_pub e
            left outer join crystal.v_eact_ce_open v on v.number_key = e.number_key
        WHERE v.data_status <> e.data_status or v.data_status is null)
            LOOP
                SELECT number_key,
                    data_status,
                    status_class,
                    sub_type,
                    status_table
                INTO VIN_NUMBER_KEY,
                    VIN_DATA_STATUS,
                    VIN_STATUS_CLASS,
                    VIN_SUB_TYPE,
                    VIN_STATUS_TABLE
                FROM apd_base
                WHERE number_key = updateRec.number_key;


                SCSIPROD.CAGIS_EACTIVITIES.UPDATEGISAPD_FEATURES (VIN_NUMBER_KEY,
                                                                  VIN_DATA_STATUS,
                                                                  VIN_STATUS_CLASS,
                                                                  VIN_SUB_TYPE,
                                                                  VIN_STATUS_TABLE);
                COMMIT;
            END LOOP;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE(SQLCODE || ' ' || SQLERRM);
        END;
        """    
    
    def runGetNotinEact(self):
        try:
            #self._Messages += "APD , Site Element Key \n"
            self.curr = self.conn.cursor()
            self.curr.callproc("dbms_output.enable", (None,))
            
            
            self.curr.execute(self.getRec)
           
            statusVar = self.curr.var(cx_Oracle.NUMBER)
            lineVar = self.curr.var(cx_Oracle.STRING)
            
            while True:
                self.curr.callproc("dbms_output.get_line", (lineVar, statusVar))
                if statusVar.getvalue() != 0:
                    break
                #print(lineVar.getvalue())
                self._Messages += lineVar.getvalue() +'\n'
                #print(self._Messages)
           
           
            self.curr.close()

        except Exception as e:
            print(e)
            return e
    def getMessages(self):
        return self._Messages
    def runUpdateEact(self):
        try:
            self._Messages=""
            #print("attempting to update eactivities")
            self._Messages += "This is to repair eactivities"
            self.curr = self.conn.cursor()
            self.curr.callproc("dbms_output.enable", (None,))
            self.curr.execute(self.UpdateStatus)
            statusVar = self.curr.var(cx_Oracle.NUMBER)
            lineVar = self.curr.var(cx_Oracle.STRING)

            while True:
                self.curr.callproc("dbms_output.get_line", (lineVar, statusVar))
                if statusVar.getvalue() != 0:
                    break
                self._Messages += lineVar.getvalue() + '\n'


            self.conn.commit()
            self.curr.close()

            self._Messages += "\n\n Updates committed"
            print("updates committed")
        except Exception as e:
            print(e)
            return e


class WAT(Crk):
    
    def __init__(self, user, pswd, dns):

        super(WAT, self).__init__( user, pswd, dns)

        self.getRec = """
        DECLARE
    v_number_key        apd_adr.number_key%TYPE;
    v_site_element_key  apd_adr.site_element_key%TYPE;

BEGIN
    FOR insertRec in (
        select number_key
        from crystal.v_eactivities
        where number_key not in (select number_key
                        from cagis.eactivities@l_pub))
    
    
    LOOP
        select max(number_key), max(site_element_key)
        into v_number_key, v_site_element_key
        from scsiprod.apd_adr
        where number_key = insertRec.number_key
        group by number_key;
        DBMS_OUTPUT.PUT_LINE(v_number_key || ', ' || v_site_element_key);
        CAGIS_EACTIVITIES.CREATEGISAPD_FEATURES(v_site_element_key, v_number_key);
        
    END LOOP;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('No data found for: ' || v_number_key || ', ' || v_site_element_key);
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE(SQLCODE || ' ' || SQLERRM);

    
END;
        
        """
        self.UpdateStatus = """
        DECLARE
            VIN_NUMBER_KEY     CHAR (10);
            VIN_DATA_STATUS    CHAR (8);
            VIN_STATUS_CLASS   VARCHAR2 (4);
            VIN_SUB_TYPE       CHAR (8);
            VIN_STATUS_TABLE   VARCHAR2 (8);
        BEGIN
            FOR updateRec
                IN (SELECT v.number_key
                        FROM crystal.v_eactivities v, cagis.eactivities@l_pub e
                    WHERE     v.number_key = e.number_key
                            AND v.data_status <> e.data_status)
            LOOP
                SELECT number_key,
                        data_status,
                        status_class,
                        sub_type,
                        status_table
                    INTO VIN_NUMBER_KEY,
                        VIN_DATA_STATUS,
                        VIN_STATUS_CLASS,
                        VIN_SUB_TYPE,
                        VIN_STATUS_TABLE
                    FROM apd_base
                WHERE number_key = updateRec.number_key;


                SCSIPROD.CAGIS_EACTIVITIES.UPDATEGISAPD_FEATURES (VIN_NUMBER_KEY,
                                                                  VIN_DATA_STATUS,
                                                                  VIN_STATUS_CLASS,
                                                                  VIN_SUB_TYPE,
                                                                  VIN_STATUS_TABLE);
                COMMIT;
            END LOOP;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE(SQLCODE || ' ' || SQLERRM || ' ' || number_key);

        END;"""
    
