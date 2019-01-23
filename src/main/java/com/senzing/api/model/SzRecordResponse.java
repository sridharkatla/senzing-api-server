package com.senzing.api.model;

/**
 * A response object that contains entity record data.
 *
 */
public class SzRecordResponse extends SzResponseWithRawData {
  /**
   * The {@link SzEntityRecord} describing the record.
   */
  private SzEntityRecord entityRecord;

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * record data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   */
  public SzRecordResponse(SzHttpMethod httpMethod,
                          int          httpStatusCode,
                          String       selfLink) {
    this(httpMethod, httpStatusCode, selfLink, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzEntityRecord}
   * describing the record.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param data The {@link SzEntityRecord} describing the record.
   */
  public SzRecordResponse(SzHttpMethod   httpMethod,
                          int            httpStatusCode,
                          String         selfLink,
                          SzEntityRecord data)
  {
    super(httpMethod, httpStatusCode, selfLink);
    this.entityRecord = data;
  }

  /**
   * Returns the data associated with this response which is an
   * {@link SzEntityRecord}.
   *
   * @return The data associated with this response.
   */
  public SzEntityRecord getData() {
    return this.entityRecord;
  }

  /**
   * Sets the data associated with this response with an {@link SzEntityRecord}.
   *
   * @param data The {@link SzEntityRecord} describing the record.
   */
  public void setData(SzEntityRecord data) {
    this.entityRecord = data;
  }
}
