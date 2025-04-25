import com.azure.data.tables.*;
import com.azure.data.tables.models.*;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.w3c.dom.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.*;

public class SaleEventProcessor {

    private static final String BLOB_CONN_STRING = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
    private static final String TABLE_CONN_STRING = System.getenv("AZURE_TABLE_CONNECTION_STRING");
    private static final String CONTAINER_NAME = System.getenv("BLOB_CONTAINER_NAME");
    private static final String BLOB_NAME = System.getenv("BLOB_NAME");
    private static final String TABLE_NAME = System.getenv("TABLE_NAME");

    public static void main(String[] args) throws Exception {
        String xml = downloadBlobXml();
        List<SaleEventWithBlob> events = extractSaleEvents(xml);
        uploadEvents(events);
        System.out.println("Done.");
    }

    private static String downloadBlobXml() {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(BLOB_CONN_STRING).buildClient();

        BlobClient blobClient = blobServiceClient
                .getBlobContainerClient(CONTAINER_NAME)
                .getBlobClient(BLOB_NAME);

        BlobDownloadContentResponse content = blobClient.downloadContent();
        return content.getContent().toString();
    }

    private static List<SaleEventWithBlob> extractSaleEvents(String xmlContent) throws Exception {
        List<SaleEventWithBlob> result = new ArrayList<>();

        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream is = new ByteArrayInputStream(xmlContent.getBytes());
        Document doc = builder.parse(is);

        NodeList saleEvents = doc.getElementsByTagName("SaleEvent");
        for (int i = 0; i < saleEvents.getLength(); i++) {
            Element saleEvent = (Element) saleEvents.item(i);

            String transactionId = getTagValue(saleEvent, "TransactionID");
            String cashierId = getTagValue(saleEvent, "CashierID");
            String registerId = getTagValue(saleEvent, "RegisterID");
            String startTime = getTagValue(saleEvent, "EventStartDate") + " " + getTagValue(saleEvent, "EventStartTime");
            String endTime = getTagValue(saleEvent, "EventEndDate") + " " + getTagValue(saleEvent, "EventEndTime");
            String total = getNestedTagValue(saleEvent, "TransactionSummary", "TransactionTotalGrandAmount");
            String outsideSales = getAttributeValue(saleEvent, "OutsideSalesFlag", "value");
            String loyaltyTxnId = getTagValue(saleEvent, "LoyaltyTransactionID");
            String storeId = getTagValue(saleEvent, "StoreHierarchyID");

            String blobFileName = "saleevent-" + transactionId + ".xml";
            String blobXml = nodeToString(saleEvent);

            TableEntity entity = new TableEntity("Store" + storeId, transactionId);
            entity.addProperty("TransactionID", transactionId);
            entity.addProperty("CashierID", cashierId);
            entity.addProperty("RegisterID", registerId);
            entity.addProperty("StartTime", startTime);
            entity.addProperty("EndTime", endTime);
            entity.addProperty("TotalAmount", total);
            entity.addProperty("OutsideSales", outsideSales);
            entity.addProperty("LoyaltyTxnID", loyaltyTxnId);
            entity.addProperty("retryCount", 0);
            entity.addProperty("status", "pending");
            entity.addProperty("blobName", blobFileName);

            result.add(new SaleEventWithBlob(entity, blobFileName, blobXml));
        }

        return result;
    }

    private static void uploadEvents(List<SaleEventWithBlob> events) {
        TableClient tableClient = new TableClientBuilder()
                .connectionString(TABLE_CONN_STRING)
                .tableName(TABLE_NAME).buildClient();

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(BLOB_CONN_STRING).buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(CONTAINER_NAME);

        for (SaleEventWithBlob event : events) {
            tableClient.createEntity(event.entity);

            BlobClient blobClient = containerClient.getBlobClient(event.blobName);
            blobClient.upload(BinaryData.fromString(event.blobXml), true);
        }
    }

    // === Helpers ===

    private static String getTagValue(Element parent, String tagName) {
        NodeList nl = parent.getElementsByTagName(tagName);
        return (nl.getLength() > 0 && nl.item(0).getTextContent() != null) ? nl.item(0).getTextContent() : "";
    }

    private static String getNestedTagValue(Element parent, String outerTag, String innerTag) {
        NodeList outer = parent.getElementsByTagName(outerTag);
        if (outer.getLength() > 0) {
            Element inner = (Element) outer.item(0);
            return getTagValue(inner, innerTag);
        }
        return "";
    }

    private static String getAttributeValue(Element parent, String tagName, String attrName) {
        NodeList nl = parent.getElementsByTagName(tagName);
        if (nl.getLength() > 0) {
            Element e = (Element) nl.item(0);
            return e.getAttribute(attrName);
        }
        return "";
    }

    private static String nodeToString(Node node) throws TransformerException {
        StringWriter writer = new StringWriter();
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        transformer.transform(new DOMSource(node), new StreamResult(writer));
        return writer.toString();
    }

    static class SaleEventWithBlob {
        public TableEntity entity;
        public String blobName;
        public String blobXml;

        public SaleEventWithBlob(TableEntity entity, String blobName, String blobXml) {
            this.entity = entity;
            this.blobName = blobName;
            this.blobXml = blobXml;
        }
    }
}
