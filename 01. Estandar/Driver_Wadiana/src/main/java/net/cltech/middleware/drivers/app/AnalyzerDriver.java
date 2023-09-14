/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.cltech.middleware.drivers.app;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import net.cltech.middleware.domain.ConnectionConfiguration;
import net.cltech.middleware.domain.Order;
import net.cltech.middleware.domain.Test;
import net.cltech.middleware.interfaces.Driver;
import net.cltech.middleware.interfaces.DriverManagerListener;
import net.cltech.middleware.interfaces.MLLPConstants;
import net.cltech.middleware.tools.Tools;

/**
 * Interfaz del equipo Wadiana
 *
 * @version 1.0.0
 * @author bcortes
 * @since 05-09-2023
 * @see Creacion
 */
public class AnalyzerDriver implements Driver
{

    private String connectionID;
    private String connectionName;
    private String option1, option2, option3, option4;
    private boolean updateConfig;
    private boolean isRunning;
    private DriverManagerListener listener;
    private boolean isQC, isLoadSheet, isMicrobiology, withImages;
    private String buffer, flag;
    private boolean isQuery, isResult;
    private final String QRY_MSH = "MSH|^~\\&|{connectionID}|{connectionName}|||{date}||QRY^Q02|1|P|2.5.1||||||ASCII|||" + MLLPConstants.CR;
    private final String QRY_QRD = "QRD|{date}|R|D|3|||RD|{order}|OTH|||T|" + MLLPConstants.CR;
    private final String QRY_QRF = "QRF||||||||||" + MLLPConstants.CR;
    private final String ORU_MSH = "MSH|^~\\&|{connectionID}|{connectionName}|||{date}||ORU^R01|1|P|2.5.1||||||ASCII|||" + MLLPConstants.CR;
    private final String ORU_PID = "PID|1" + MLLPConstants.CR;
    private final String ORU_OBR = "OBR|(index)|{order}|||||||||||||||||||||||||||||||||||||||||||{QC}|||" + MLLPConstants.CR;
    private final String ORU_OBX = "OBX|{index}|NM||{test}|{result}|||||F|||||||||{dateResult}" + MLLPConstants.CR;
    private final SimpleDateFormat yyyyMMddHHmmss;
    private ConnectionConfiguration connectionConfiguration;
    private ConnectionConfiguration connectionConfigurationTemp;
    private int index, indexTraceSend;
    private int indexOBR;
    private String hl7Message;
    private int indexO;
    private int indexP;

    private char state;
    private List<String> ordersToSend, tracesToSend;
    private Order order;
    private final String HEADER = "H|\\^&|||QUANTA Link|||||||P|1394-91|20170516130000" + MLLPConstants.CR + MLLPConstants.ETX;
    private final String PATIENT = "P|{index}||||{lastName}^{firstName}||{birthDate}|{gender}|||||" + MLLPConstants.CR + MLLPConstants.ETX;
    private final String ORDER = "O|{index}|{order}||^^{testCode}|R|{date}|||||A||||||||||||||" + MLLPConstants.CR + MLLPConstants.ETX;
    private final String ENDLINE = "L|1|{flag}" + MLLPConstants.CR + MLLPConstants.ETX;
    private String orderR;

    /**
     * constructor de la clase
     */
    public AnalyzerDriver()
    {
        connectionID = null;
        connectionName = null;
        option1 = null;
        listener = null;
        isQC = false;
        buffer = "";
        isQuery = false;
        yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");
    }

    /**
     * metodo para la recepcion de tramas
     *
     * @param stream datos recibidos por tcp
     *
     */
    @Override
    public void receiveTCPMessage(String stream)
    {
        buffer += stream;
        if (buffer.contains(String.valueOf(MLLPConstants.ENQ))
                || buffer.contains(String.valueOf(MLLPConstants.ACK))
                || buffer.contains(MLLPConstants.NAK)
                || buffer.contains(MLLPConstants.LF)
                || buffer.contains(MLLPConstants.EOT))
        {
            listener.registerTrace("[ANALYZER] : " + Tools.convertString(buffer));
            String temData = buffer;
            buffer = "";
            analyzerTrace(temData);
        }
    }

    /**
     * analiza las tramas que lelgan del analizador dependiendo de su
     * composicion
     *
     * @param trace segemnto de trama astm
     *
     */
    private void analyzerTrace(String trace)
    {
        if (trace.contains(MLLPConstants.ENQ))
        { //Pregunta desde el equipo
            listener.sendTCPMessage(MLLPConstants.ACK);
        } else if (trace.contains(MLLPConstants.ACK))
        {
            //Equipo envia respuesta positiva
            sendPosition();
        } else if (trace.contains(MLLPConstants.NAK))
        {
            switch (state)
            {
                case '2':
                    state = 'P';
                    break;
                case 'T':
                    state = 'P';
                    break;
            }
            listener.sendTCPMessage(String.valueOf(MLLPConstants.ENQ));
        } else if (trace.contains(MLLPConstants.EOT))
        { //Finaliza el envio de bloque el analizador
            if (isQuery)
            { //Si es query
                listener.sendTCPMessage(MLLPConstants.ENQ);
                updateConfiguration();
                isQuery = false;
                isResult = false;
            } else if (isResult)
            {
                new UpdateWorker(hl7Message).start();
                isResult = false;
            }
        } else if (trace.contains(MLLPConstants.LF))
        { //Fin de envio de cada segmento
            analyzerSegment(trace);
        }
    }

    /**
     * analiza los segmentos de trama astm para armar el mensaje hl7 que se
     * envia al middleware
     *
     * @param segment segmento de trama astm
     */
    private void analyzerSegment(String segment)
    {
        switch (segment.charAt(2))
        {
            case 'H':   //Cabecera
                isQuery = false;
                isResult = false;
                orderR = "";
                setHeader();
                break;
            case 'L':   //Paciente
                break;
            case 'O':   //Orden
                if (orderR != null && !orderR.isEmpty())
                {
                    new UpdateWorker(hl7Message).start();
                    hl7Message = "";
                    setHeader();
                    orderR = null;
                }
                String obr = ORU_OBR;
                int beginIndex = Tools.positionChar(segment, '|', 2) + 1;
                int endIndex = Tools.positionChar(segment, '|', 3);
                orderR = segment.substring(beginIndex, endIndex).trim();
                listener.registerTrace("[ORDER] : " + orderR);
                obr = obr.replace("(index)", String.valueOf(indexOBR));
                obr = obr.replace("{order}", orderR);
                obr = obr.replace("{QC}", "");
                indexOBR++;
                hl7Message += obr;
                break;
            case 'R':   //Resultado
                if (!orderR.isEmpty())
                {
                    isResult = true;
                    beginIndex = Tools.positionChar(segment, '|', 2) + 1;
                    endIndex = Tools.positionChar(segment, '|', 3);
                    String nTest = segment.substring(beginIndex, endIndex).trim();

                    beginIndex = Tools.positionChar(segment, '|', 4) + 1;
                    endIndex = Tools.positionChar(segment, '|', 5);
                    String type = segment.substring(beginIndex, endIndex).trim();
                    try
                    {
                        nTest = nTest.contains("^") ? nTest.split("\\^")[2].trim() : nTest;
                    } catch (Exception e)
                    {
                        nTest = "";
                        listener.registerTrace("TEST NOT FOUND");
                    }
                    nTest = getHomologationTest(nTest, type);
                    beginIndex = Tools.positionChar(segment, '|', 3) + 1;
                    endIndex = Tools.positionChar(segment, '|', 4);
                    String nResult = segment.substring(beginIndex, endIndex).trim();
                    if (!nResult.isEmpty() && !nTest.isEmpty())
                    {
                        listener.registerTrace("[TEST] : " + nTest + " [RESULT] : " + nResult);
                        String obx = ORU_OBX;
                        obx = obx.replace("{index}", String.valueOf(index));
                        obx = obx.replace("{test}", nTest);
                        obx = obx.replace("{result}", nResult);
                        obx = obx.replace("{dateResult}", yyyyMMddHHmmss.format(new Date()));
                        index++;
                        if (index >= 3)
                        {
                            index = 1;
                        }
                        hl7Message += obx;
                    } else
                    {
                        listener.registerTrace("RESULT IS EMPTY OR TEST IS EMPTY");
                    }
                }
                break;
            case 'Q':   //Query
                isQuery = true;
                isResult = false;
                sendRequest(segment);
                break;
        }
        listener.sendTCPMessage(MLLPConstants.ACK);
    }

    public void sendRequest(String dataReceive)
    {
        try
        {

            int beginIndex = Tools.positionChar(dataReceive, '|', 2) + 1;
            int endIndex = Tools.positionChar(dataReceive, '|', 3);
            String orderQuey = dataReceive.substring(beginIndex, endIndex).trim().concat("^");
            String[] orders = orderQuey.replace("\\", "").split("\\^");
            listener.registerTrace("ORDER LENGTH: " + orders.length);
            if (orders.length > 0)
            {
                tracesToSend = new ArrayList<>();
                String traceSend;
                index = 0;
                traceSend = getIndex() + HEADER;
                traceSend = MLLPConstants.STX + traceSend + Tools.getBCC(traceSend) + MLLPConstants.CR + MLLPConstants.LF;
                tracesToSend.add(traceSend);
                addOrder(index, orders);
                String end = getIndex() + ENDLINE;
                end = end.replace("{flag}", flag);
                end = MLLPConstants.STX + end + Tools.getBCC(end) + MLLPConstants.CR + MLLPConstants.LF;
                tracesToSend.add(end);
            } else
            {
                listener.registerTrace("ORDER IS EMPTY");
            }
        } catch (NumberFormatException ex)
        {
            listener.registerTrace("ERROR" + ex);
        }
    }

    private int getIndex()
    {
        index++;
        if (index > 7)
        {
            index = 0;
        }
        return index;
    }

    private void checkQueryTrace(String orderR)
    {
        try
        {
            if (orderR != null && !orderR.isEmpty())
            {
                if (ordersToSend == null)
                {
                    ordersToSend = new ArrayList<>();
                }
                index = 1;
                if (order != null)
                {
                    String patient = PATIENT;
                    String testTrace = "";
                    patient = patient.replace("{index}", String.valueOf(indexP));
                    patient = patient.replace("{patientID}", order.getPatientId());
                    patient = patient.replace("{lastname}", order.getPatientLastName());
                    patient = patient.replace("{name}", order.getPatientNames());
                    patient = patient.replace("{currentDateTime}", yyyyMMddHHmmss.format(new Date()));
                    patient = patient.replace("{sex}", order.getSex());
                    ordersToSend.add(patient);
                    testTrace = "";

                    for (String test : order.getTests())
                    {
                        String order = ORDER;
                        order = order.replace("{index}", String.valueOf(indexO));
                        order = order.replace("{order}", orderR != null ? orderR : "");
                        order = order.replace("{tests}", test);
                        ordersToSend.add(order);
                        indexO++;
                    }
                    testTrace = !testTrace.trim().isEmpty() ? testTrace.substring(0, testTrace.length() - 1) : testTrace;
                    indexP++;
                }
            }
        } catch (Exception e)
        {
            listener.registerError(e.getMessage(), e);
        }
    }

    //<editor-fold defaultstate="collapsed" desc="Metodos obligatorios">
    @Override
    public void setOption1(String option1)
    {
        this.option1 = option1;
    }

    @Override
    public void setOption2(String option2)
    {
        this.option2 = option2;
    }

    @Override
    public void setConnectionID(String connectionID)
    {
        this.connectionID = connectionID;
    }

    @Override
    public void setConnectionName(String connectionName)
    {
        this.connectionName = connectionName;
    }

    @Override
    public void setDriverManagerListener(DriverManagerListener listener)
    {
        this.listener = listener;
    }

    @Override
    public void setQC(boolean qc)
    {
        this.isQC = qc;
    }
//</editor-fold>

    /**
     * invoca la creacion de la entidad Order
     *
     * @param hl7Response mensaje de respuesta del middleware
     *
     */
    private void createPatient(String hl7Response)
    {
        try
        {
            order = Tools.createOrder(hl7Response);
        } catch (Exception ex)
        {
            listener.registerError("ERROR", ex);
        }
    }

    /**
     * arma las tramas astm para el envio hacia el analizador
     *
     */
    private void sendPosition()
    {
        try
        {
            if (tracesToSend != null && !tracesToSend.isEmpty() && indexTraceSend < tracesToSend.size())
            {
                Thread.sleep(200);
                listener.sendTCPMessage(tracesToSend.get(indexTraceSend));
                indexTraceSend++;
            } else
            {
                listener.sendTCPMessage(MLLPConstants.EOT);
                if ("I".equals(flag))
                {
                    listener.registerTrace("ORDERS HAS BEEN SEND");
                } else
                {
                    listener.registerTrace("ORDERS NOT FOUND");
                }
                indexTraceSend = 0;
                tracesToSend = null;
                isQuery = false;
                flag = "I";
            }
        } catch (InterruptedException e)
        {
            listener.registerTrace("ERROR EN SENDPOSITION: " + e);
        }
    }

    @Override
    public void setConnectionConfiguration(ConnectionConfiguration connectionConfiguration)
    {
        listener.registerTrace("IS RUNNING 0");
        if (isRunning)
        {
            listener.registerTrace("IS RUNNING 1");
            connectionConfigurationTemp = connectionConfiguration;
            updateConfig = true;
        } else
        {
            this.connectionConfiguration = connectionConfiguration;
        }
    }

    /**
     * Actualiza la confirguación de la interfaz desde la configuración recibida
     * del servidor
     */
    private void updateConfiguration()
    {
        listener.registerTrace("ENTRO 0");
        if (updateConfig)
        {
            listener.registerTrace("ENTRO 1");
            if (connectionConfigurationTemp != null)
            {
                connectionConfiguration = connectionConfigurationTemp;
                listener.registerTrace("CONECCTION CONFIGURATION : " + connectionConfiguration.getTests());
                connectionConfigurationTemp = null;
                updateConfig = false;
            }
        }
    }

    private void setHeader()
    {
        String msh = ORU_MSH;
        msh = msh.replace("{connectionID}", connectionID);
        msh = msh.replace("{connectionName}", connectionName);
        msh = msh.replace("{date}", yyyyMMddHHmmss.format(new Date()));
        hl7Message += msh;
        String pid = ORU_PID;
        hl7Message += pid;
    }

    private void addOrder(int index, String[] orders)
    {
        String sampler = null;
        String prescriptionNumber = null;
        int indexPatient = 1;
        for (String orderQ : orders)
        {
            hl7Message = "";
            if (!orderQ.isEmpty())
            {
                orderQ = orderQ.replace("^", "");
                listener.registerTrace("ORDER TO PROGRAM: " + orderQ);
                String msh2 = QRY_MSH;
                msh2 = msh2.replace("{connectionID}", connectionID);
                msh2 = msh2.replace("{connectionName}", connectionName);
                msh2 = msh2.replace("{date}", yyyyMMddHHmmss.format(new Date()));
                hl7Message += msh2;
                String qrd = QRY_QRD;
                qrd = qrd.replace("{date}", yyyyMMddHHmmss.format(new Date()));
                qrd = qrd.replace("{order}", orderQ);
                String qrf = QRY_QRF;
                hl7Message += qrd + qrf;
                indexPatient = indexPatient + getTests(hl7Message, orderQ, index, indexPatient);
                sampler = null;
                prescriptionNumber = null;
            }
        }
    }

    private int getTests(String hl7Message, String orderQ, int index, int indexPatient)
    {
        listener.registerTrace("HL7 QUERY [" + hl7Message + "]");
        String hl7Response = listener.queryOrder(hl7Message);
        listener.registerTrace("HL7 RESPONSE [" + hl7Response + "]");
        createPatient(hl7Response);
        if (order != null)
        {
            List<Test> tests = connectionConfiguration.getConfigTests(order.getTests());
            if (!tests.isEmpty())
            {
                flag = "F";
                String patient = getIndex() + PATIENT;
                String birthDate = order.getPatientBirthDate();
                if (birthDate.contains("T"))
                {
                    birthDate = birthDate.substring(0, birthDate.indexOf("T")).replace("-", "");
                } else
                {
                    birthDate = birthDate.replace("-", "");
                }
                patient = patient.replace("{index}", "" + indexPatient);
                patient = patient.replace("{lastName}", order.getPatientLastName());
                patient = patient.replace("{firstName}", order.getPatientNames());
                patient = patient.replace("{birthDate}", birthDate);
                patient = patient.replace("{gender}", order.getSex().equals("1") ? "M" : "F");
                patient = MLLPConstants.STX + patient + Tools.getBCC(patient) + MLLPConstants.CR + MLLPConstants.LF;
                tracesToSend.add(patient);
                String order = null;
                int indexOrder = 1;
                for (Test test : tests)
                {
                    order = getIndex() + ORDER;
                    order = order.replace("{index}", String.valueOf(indexOrder));
                    order = order.replace("{order}", orderQ);
                    order = order.replace("{testCode}", test.getCdc2());
                    order = order.replace("{date}", new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
                    order = MLLPConstants.STX + order + Tools.getBCC(order) + MLLPConstants.CR + MLLPConstants.LF;
                    tracesToSend.add(order);
                    indexOrder++;
                }
                return 1;
            } else
            {
                listener.registerTrace("TEST FOR ORDER : ".concat(orderQ).concat(" NOT FOUND"));
            }
        }
        return 0;
    }

    private String getHomologationTest(String nTest, String type)
    {
        List<Test> tests = connectionConfiguration.getTests();
        if (type.contains("Interpret"))
        {
            for (Test test : tests)
            {
                if (test.getCdc2().equals(nTest) && type.contains(test.getCdc3()))
                {
                    return test.getCodTestConnect();
                }
            }
        } else
        {
            for (Test test : tests)
            {
                if (test.getCdc2().equals(nTest) && !test.getCdc3().equals("Interpret"))
                {
                    return test.getCodTestConnect();
                }
            }
        }
        return "";
    }

    /**
     * clase para el envio de resultados hacia el middleware
     *
     */
    class UpdateWorker extends Thread
    {

        private String hl7MessageR;

        public UpdateWorker(String hl7Message)
        {
            this.hl7MessageR = hl7Message;
        }

        @Override
        public void run()
        {
            try
            {
                listener.registerTrace("[REQUEST] : " + Tools.convertString(hl7MessageR));
                if (hl7MessageR != null && !hl7MessageR.isEmpty())
                {
                    String hl7Response = listener.sendResult(hl7MessageR);
                    listener.registerTrace("[RESPONSE] : " + hl7Response);
                } else
                {
                    listener.registerTrace("No llegaron resultados");
                }
            } catch (Exception ex)
            {
                listener.registerError("ERROR : ", ex);
            } finally
            {
                done();
            }
        }

        private void done()
        {
            hl7MessageR = null;
            isRunning = false;
        }
    }

    @Override
    public void setOption3(String option3)
    {
        this.option3 = option3;
    }

    @Override
    public void setOption4(String option4)
    {
        this.option4 = option4;
    }

    @Override
    public void sendOrder(List<String> list)
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setLoadSheet(boolean isLoadSheet)
    {
        this.isLoadSheet = isLoadSheet;
    }

    @Override
    public void setMicrobiology(boolean isMicrobiology)
    {
        this.isMicrobiology = isMicrobiology;
    }

    public String getOption3()
    {
        return option3;
    }

    public String getOption4()
    {
        return option4;
    }

    public boolean isIsLoadSheet()
    {
        return isLoadSheet;
    }

    public boolean isIsMicrobiology()
    {
        return isMicrobiology;
    }

    public boolean isWithImages()
    {
        return withImages;
    }

    public void setWithImages(boolean withImages)
    {
        this.withImages = withImages;
    }

    @Override
    public void closeTasks()
    {
    }
}
