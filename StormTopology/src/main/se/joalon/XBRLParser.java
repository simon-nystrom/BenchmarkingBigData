
package se.joalon;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public class XBRLParser {

    private DocumentBuilderFactory documentBuilderFactory;
    private DocumentBuilder documentBuilder;
    private Document document;

    public XBRLParser() {

        try {
            documentBuilderFactory = DocumentBuilderFactory.newInstance();
            documentBuilder = documentBuilderFactory.newDocumentBuilder();
        } catch (ParserConfigurationException ex) {
            System.out.println("XML Parser config error!");
        }
    }

    public void parseAndStoreDocument(String xml) {
        try {
            document = documentBuilder.parse(new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8"))));
        } catch (IOException ioe) {
            document = null;
            System.out.println("Error parsing XBRL document, IOException");
        } catch (SAXException ex) {
            document = null;
            System.out.println("Error parsing XBRL document, SAXException");
        }
    }

    public int countByTag(String tag) {
        return document.getElementsByTagName(tag).getLength();
    }

}