// TODO: add startTimeSec - integer epoch seconds only - why startTime does not apply?
// TODO: buffer file writes to bigger chunks?
package kg.apc.jmeter.reporters;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import kg.apc.jmeter.JMeterPluginsUtils;

import org.apache.jmeter.engine.util.NoThreadClone;
import org.apache.jmeter.reporters.AbstractListenerElement;
import org.apache.jmeter.reporters.ResultCollector;
import org.apache.jmeter.samplers.Remoteable;
import org.apache.jmeter.samplers.SampleEvent;
import org.apache.jmeter.samplers.SampleListener;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

/**
 * @see ResultCollector
 */
public class FlexibleFileWriter
        extends AbstractListenerElement
        implements SampleListener, Serializable,
        TestStateListener, Remoteable, NoThreadClone {

    public static final String AVAILABLE_FIELDS = "isSuccsessful "
            + "startTime endTime "
            + "sentBytes receivedBytes "
            + "responseTime latency "
            + "responseCode responseMessage "
            + "isFailed " // surrogates
            + "threadName sampleLabel "
            + "startTimeMillis endTimeMillis "
            + "responseTimeMicros latencyMicros "
            + "requestData responseData responseHeaders "
            + "threadsCount ";
    private static final Logger log = LoggingManager.getLoggerForClass();
    private static final String OVERWRITE = "overwrite";
    private static final String FILENAME = "filename";
    private static final String COLUMNS = "columns";
    private static final String HEADER = "header";
    private static final String FOOTER = "footer";
    private static final String VAR_PREFIX = "variable#";
    private static final String WRITE_BUFFER_LEN_PROPERTY = "kg.apc.jmeter.reporters.FFWBufferSize";
    private final int writeBufferSize = JMeterUtils.getPropDefault(WRITE_BUFFER_LEN_PROPERTY, 1024 * 10);
    protected volatile FileChannel fileChannel;
    private int[] compiledVars;
    private int[] compiledFields;
    private ByteBuffer[] compiledConsts;
    private final ArrayList<String> availableFieldNames = new ArrayList<String>(Arrays.asList(AVAILABLE_FIELDS.trim().split(" ")));
    private static final byte[] b1 = "1".getBytes();
    private static final byte[] b0 = "0".getBytes();

    private static final Map<String, Object[]> FILE_CHANNEL_AND_REFERENCE_COUNT_BY_FILENAME = new HashMap<String, Object[]>();

    public FlexibleFileWriter() {
        super();
    }

    @Override
    public void sampleStarted(final SampleEvent e) {
    }

    @Override
    public void sampleStopped(final SampleEvent e) {
    }

    @Override
    public void testStarted() {
        this.compileColumns();
        try {
            this.openFile();
        } catch (final FileNotFoundException ex) {
            log.error("Cannot open file " + this.getFilename(), ex);
        } catch (final IOException ex) {
            log.error("Cannot write file header " + this.getFilename(), ex);
        }
    }

    @Override
    public void testStarted(final String host) {
        this.testStarted();
    }

    @Override
    public void testEnded() {
        this.closeFile();
    }

    @Override
    public void testEnded(final String host) {
        this.testEnded();
    }

    public void setFilename(final String name) {
        this.setProperty(FILENAME, name);
    }

    public String getFilename() {
        return this.getPropertyAsString(FILENAME);
    }

    public void setColumns(final String cols) {
        this.setProperty(COLUMNS, cols);
    }

    public String getColumns() {
        return this.getPropertyAsString(COLUMNS);
    }

    public boolean isOverwrite() {
        return this.getPropertyAsBoolean(OVERWRITE, false);
    }

    public void setOverwrite(final boolean ov) {
        this.setProperty(OVERWRITE, ov);
    }

    public void setFileHeader(final String str) {
        this.setProperty(HEADER, str);
    }

    public String getFileHeader() {
        return this.getPropertyAsString(HEADER);
    }

    public void setFileFooter(final String str) {
        this.setProperty(FOOTER, str);
    }

    public String getFileFooter() {
        return this.getPropertyAsString(FOOTER);
    }

    /**
     * making this once to be efficient and avoid manipulating strings in switch
     * operators
     */
    private void compileColumns() {
        log.debug("Compiling columns string: " + this.getColumns());
        final String[] chunks = JMeterPluginsUtils.replaceRNT(this.getColumns()).split("\\|");
        log.debug("Chunks " + chunks.length);
        this.compiledFields = new int[chunks.length];
        this.compiledVars = new int[chunks.length];
        this.compiledConsts = new ByteBuffer[chunks.length];
        for (int n = 0; n < chunks.length; n++) {
            final int fieldID = this.availableFieldNames.indexOf(chunks[n]);
            if (fieldID >= 0) {
                //log.debug(chunks[n] + " field id: " + fieldID);
                this.compiledFields[n] = fieldID;
            } else {
                this.compiledFields[n] = -1;
                this.compiledVars[n] = -1;
                if (chunks[n].contains(VAR_PREFIX)) {
                    log.debug(chunks[n] + " is sample variable");
                    final String varN = chunks[n].substring(VAR_PREFIX.length());
                    try {
                        this.compiledVars[n] = Integer.parseInt(varN);
                    } catch (final NumberFormatException e) {
                        log.error("Seems it is not variable spec: " + chunks[n]);
                        this.compiledConsts[n] = ByteBuffer.wrap(chunks[n].getBytes());
                    }
                } else {
                    log.debug(chunks[n] + " is const");
                    if (chunks[n].length() == 0) {
                        //log.debug("Empty const, treated as |");
                        chunks[n] = "|";
                    }

                    this.compiledConsts[n] = ByteBuffer.wrap(chunks[n].getBytes());
                }
            }
        }
    }

    protected void openFile() throws IOException {
        final String filename = this.getFilename();

        final Object[] entry = FILE_CHANNEL_AND_REFERENCE_COUNT_BY_FILENAME.get(filename);

        synchronized(FlexibleFileWriter.class)
        {
            if(entry != null && entry.length == 2 && entry[0] instanceof FileChannel && entry[1] instanceof AtomicInteger)
            {
                this.fileChannel = (FileChannel)entry[0];
                ((AtomicInteger)entry[1]).incrementAndGet();
            }
            else
            {
                final FileOutputStream fos = new FileOutputStream(filename, !this.isOverwrite());
                this.fileChannel = fos.getChannel();

                final String header = JMeterPluginsUtils.replaceRNT(this.getFileHeader());
                if (!header.isEmpty()) {
                    this.fileChannel.write(ByteBuffer.wrap(header.getBytes()));
                }

                FILE_CHANNEL_AND_REFERENCE_COUNT_BY_FILENAME.put(filename, new Object[]{this.fileChannel, new AtomicInteger(1)});
            }
        }
    }

    private synchronized void closeFile() {
        synchronized(FlexibleFileWriter.class)
        {
            if (this.fileChannel != null && this.fileChannel.isOpen()) {

                final String filename = this.getFilename();

                final Object[] entry = FILE_CHANNEL_AND_REFERENCE_COUNT_BY_FILENAME.get(filename);

                boolean doClose = true;
                if(entry != null && entry.length == 2 && entry[0] instanceof FileChannel && entry[1] instanceof AtomicInteger)
                {
                    if(((AtomicInteger)entry[1]).decrementAndGet() > 0)
                    {
                        doClose = false;
                        FILE_CHANNEL_AND_REFERENCE_COUNT_BY_FILENAME.remove(filename);
                    }
                }

                if (doClose)
                {
                    try {
                        final String footer = JMeterPluginsUtils.replaceRNT(this.getFileFooter());
                        if (!footer.isEmpty()) {
                            this.fileChannel.write(ByteBuffer.wrap(footer.getBytes()));
                        }

                        this.fileChannel.force(false);
                        this.fileChannel.close();
                    } catch (final IOException ex) {
                        log.error("Failed to close file: " + filename, ex);
                    }
                }
            }
        }
    }

    @Override
    public void sampleOccurred(final SampleEvent evt) {
        if (this.fileChannel == null || !this.fileChannel.isOpen()) {
            if (log.isWarnEnabled()) {
                log.warn("File writer is closed! Maybe test has already been stopped");
            }
            return;
        }

        final ByteBuffer buf = ByteBuffer.allocateDirect(this.writeBufferSize);
        for (int n = 0; n < this.compiledConsts.length; n++) {
            if (this.compiledConsts[n] != null) {
                synchronized (this.compiledConsts) {
                    buf.put(this.compiledConsts[n].duplicate());
                }
            } else {
                if (!this.appendSampleResultField(buf, evt.getResult(), this.compiledFields[n])) {
                    this.appendSampleVariable(buf, evt, this.compiledVars[n]);
                }
            }
        }

        buf.flip();

        try {
            this.syncWrite(buf);
        } catch (final IOException ex) {
            log.error("Problems writing to file", ex);
        }
    }

    private void syncWrite(final ByteBuffer buf) throws IOException {
        synchronized (this.fileChannel)
        {
            final FileLock lock = this.fileChannel.lock();
            try
            {
                this.fileChannel.write(buf);
            }
            finally
            {
                lock.release();
            }
        }
    }

    /*
     * we work with timestamps, so we assume number > 1000 to avoid tests
     * to be faster
     */
    private String getShiftDecimal(final long number, final int shift) {
        final StringBuilder builder = new StringBuilder();
        builder.append(number);

        final int index = builder.length() - shift;
        builder.insert(index, ".");

        return builder.toString();
    }

    private void appendSampleVariable(final ByteBuffer buf, final SampleEvent evt, final int varID) {
        if (SampleEvent.getVarCount() < varID + 1) {
            buf.put(("UNDEFINED_variable#" + varID).getBytes());
            log.warn("variable#" + varID + " does not exist!");
        } else {
            if (evt.getVarValue(varID) != null) {
                buf.put(evt.getVarValue(varID).getBytes());
            }
        }
    }

    /**
     * @return boolean true if existing field found, false instead
     */
    private boolean appendSampleResultField(final ByteBuffer buf, final SampleResult result, final int fieldID) {
        // IMPORTANT: keep this as fast as possible
        switch (fieldID) {
            case 0:
                buf.put(result.isSuccessful() ? b1 : b0);
                break;

            case 1:
                buf.put(String.valueOf(result.getStartTime()).getBytes());
                break;

            case 2:
                buf.put(String.valueOf(result.getEndTime()).getBytes());
                break;

            case 3:
                if (result.getSamplerData() != null) {
                    buf.put(String.valueOf(result.getSamplerData().length()).getBytes());
                } else {
                    buf.put(b0);
                }
                break;

            case 4:
                if (result.getResponseData() != null) {
                    buf.put(String.valueOf(result.getResponseData().length).getBytes());
                } else {
                    buf.put(b0);
                }
                break;

            case 5:
                buf.put(String.valueOf(result.getTime()).getBytes());
                break;

            case 6:
                buf.put(String.valueOf(result.getLatency()).getBytes());
                break;

            case 7:
                buf.put(result.getResponseCode().getBytes());
                break;

            case 8:
                buf.put(result.getResponseMessage().getBytes());
                break;

            case 9:
                buf.put(!result.isSuccessful() ? b1 : b0);
                break;

            case 10:
                buf.put(result.getThreadName().getBytes());
                break;

            case 11:
                buf.put(result.getSampleLabel().getBytes());
                break;

            case 12:
                buf.put(this.getShiftDecimal(result.getStartTime(), 3).getBytes());
                break;

            case 13:
                buf.put(this.getShiftDecimal(result.getEndTime(), 3).getBytes());
                break;

            case 14:
                buf.put(String.valueOf(result.getTime() * 1000).getBytes());
                break;

            case 15:
                buf.put(String.valueOf(result.getLatency() * 1000).getBytes());
                break;

            case 16:
                if (result.getSamplerData() != null) {
                    buf.put(result.getSamplerData().getBytes());
                } else {
                    buf.put(b0);
                }
                break;

            case 17:
                buf.put(result.getResponseData());
                break;

            case 18:
                buf.put(result.getRequestHeaders().getBytes());
                break;

            case 19:
                buf.put(String.valueOf(result.getAllThreads()).getBytes());
                break;

            default:
                return false;
        }
        return true;
    }
}
