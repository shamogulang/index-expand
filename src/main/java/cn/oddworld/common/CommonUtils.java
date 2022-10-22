package cn.oddworld.common;

import cn.oddworld.store.Message;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

public class CommonUtils {



    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public static void string2File(final String str, final String fileName) throws IOException {

        // write content to the tmp file
        String tmpFile = fileName + ".tmp";
        string2FileNotSafe(str, tmpFile);

        String bakFile = fileName + ".bak";
        // read content from filename
        String prevContent = file2String(fileName);
        if (prevContent != null) {
            // write content to bak file, store last file content
            string2FileNotSafe(prevContent, bakFile);
        }

        // delete filename if exist
        File file = new File(fileName);
        file.delete();

        // create tmp file
        file = new File(tmpFile);
        // rename tmp file to file
        file.renameTo(new File(fileName));
    }

    public static String file2String(final String fileName) throws IOException {
        File file = new File(fileName);
        return file2String(file);
    }

    public static String file2String(final File file) throws IOException {
        if (file.exists()) {
            byte[] data = new byte[(int) file.length()];
            boolean result;

            FileInputStream inputStream = null;
            try {
                inputStream = new FileInputStream(file);
                int len = inputStream.read(data);
                result = len == data.length;
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }

            if (result) {
                return new String(data);
            }
        }
        return null;
    }


    public static void string2FileNotSafe(final String str, final String fileName) throws IOException {
        File file = new File(fileName);
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            fileParent.mkdirs();
        }
        FileWriter fileWriter = null;

        try {
            fileWriter = new FileWriter(file);
            fileWriter.write(str);
        } catch (IOException e) {
            throw e;
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }


    public static Message buffer2Message(ByteBuffer buffer){

        Message message = new Message();
        final int businessLent = buffer.getInt();
        byte[] businessBody = new byte[businessLent];
        buffer.get(businessBody);
        message.setBusiness(new String(businessBody, Charset.forName(Constants.UTF8)));

        final int bodyLent = buffer.getInt();
        byte[] bodyBody = new byte[bodyLent];
        buffer.get(bodyBody);
        message.setBody(bodyBody);

        final int prosLent = buffer.getInt();
        byte[] prosBody = new byte[prosLent];
        buffer.get(prosBody);
        final String prosStrings = new String(prosBody, Charset.forName(Constants.UTF8));
        final Map<String, String> properties = CommonUtils.string2messageProperties(prosStrings);
        message.setProperties(properties);
        return message;
    }

    public static void message2Buffer(MappedByteBuffer buffer, Message message){

        // 1、setting business name info
        final String business = message.getBusiness();
        final byte[] businessBytes = business.getBytes();
        int businessBytesLent = businessBytes.length;
        buffer.putInt(businessBytesLent);
        buffer.put(businessBytes);

        // 2、setting actual content
        final byte[] body = message.getBody();
        int bodyLent = body.length;
        buffer.putInt(bodyLent);
        buffer.put(body);

        // 3、setting extra pros
        final Map<String, String> properties = message.getProperties();
        final String properties2String = CommonUtils.properties2String(properties);
        final byte[] properties2StringBytes = properties2String.getBytes();
        int prosBytesLent = properties2StringBytes.length;
        buffer.putInt(prosBytesLent);
        buffer.put(properties2StringBytes);
    }


    public static Map<String, String> string2messageProperties(final String properties) {
        Map<String, String> map = new HashMap<String, String>();
        if (properties != null) {
            String[] items = properties.split(Constants.PROS_SEPARATOR);
            for (String i : items) {
                String[] nv = i.split(Constants.NAME_VALUE_SEPARATOR);
                if (2 == nv.length) {
                    map.put(nv[0], nv[1]);
                }
            }
        }
        return map;
    }

    public static String properties2String(Map<String, String> properties) {
        StringBuilder sb = new StringBuilder();
        if (properties != null) {
            for (final Map.Entry<String, String> entry : properties.entrySet()) {
                final String name = entry.getKey();
                final String value = entry.getValue();

                if (value == null) {
                    continue;
                }
                sb.append(name);
                sb.append(Constants.NAME_VALUE_SEPARATOR);
                sb.append(value);
                sb.append(Constants.PROS_SEPARATOR);
            }
        }
        return sb.toString();
    }

}
