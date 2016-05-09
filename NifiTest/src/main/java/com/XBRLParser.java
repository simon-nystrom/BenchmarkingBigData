package com;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;


public class XBRLParser {

    private DocumentBuilderFactory documentBuilderFactory;
    private DocumentBuilder documentBuilder;
    private Document document;
    private String xml;

    public XBRLParser(String xml) throws ParserConfigurationException, IOException, SAXException {

        this.xml = xml;
        documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilder = documentBuilderFactory.newDocumentBuilder();
        document = documentBuilder.parse(new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8")) {
        }));
    }

    public int countByTag(String tag) {
        return document.getElementsByTagName(tag).getLength();

    }
}
