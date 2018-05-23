/**
 * Imgur API
 * OpenAPI spec version: 1.0
 *
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */
/* tslint:disable:no-unused-variable member-ordering */

import { Inject, Injectable, Optional } from '@angular/core';
import {
  HttpClient,
  HttpHeaders,
  HttpParams,
  HttpResponse,
  HttpEvent
} from '@angular/common/http';
import { CustomHttpUrlEncodingCodec } from '../encoder';

import { Observable } from 'rxjs';

import { BASE_PATH, COLLECTION_FORMATS } from '../variables';
import { Configuration } from '../configuration';

@Injectable()
export class AlbumService {
  protected basePath = 'https://api.imgur.com';
  public defaultHeaders = new HttpHeaders();
  public configuration = new Configuration();

  constructor(
    protected httpClient: HttpClient,
    @Optional()
    @Inject(BASE_PATH)
    basePath: string,
    @Optional() configuration: Configuration
  ) {
    if (basePath) {
      this.basePath = basePath;
    }
    if (configuration) {
      this.configuration = configuration;
      this.basePath = basePath || configuration.basePath || this.basePath;
    }
  }

  /**
   * @param consumes string[] mime-types
   * @return true: consumes contains 'multipart/form-data', false: otherwise
   */
  private canConsumeForm(consumes: string[]): boolean {
    const form = 'multipart/form-data';
    for (const consume of consumes) {
      if (form === consume) {
        return true;
      }
    }
    return false;
  }

  /**
   * Add Images to an Album (Un-Authed)
   * Takes parameter, &#x60;Deletehashes[]&#x60;, as an array of Deletehashes to add to the album. Alternatively, the &#x60;Deletehashes&#x60; parameter can take a comma delimted string of Deletehashes.  #### Response Model: [Basic](https://api.imgur.com/models/basic)  #### Parameters  | Key   | Required | Description                                           | |-------|----------|-------------------------------------------------------| | Deletehashes[] | required | The image Deletehashes that you want to be added to the album. The [] represents the ability to use this variable as an array. |
   * @param Deletehashes
   * @param albumDeleteHash
   * @param observe set whether or not to return the data Observable as the body, response or events. defaults to returning the body.
   * @param reportProgress flag to report request and response progress.
   */
  public albumAddByAlbumDeleteHashPost(
    Deletehashes: string,
    albumDeleteHash: string,
    observe?: 'body',
    reportProgress?: boolean
  ): Observable<any>;
  public albumAddByAlbumDeleteHashPost(
    Deletehashes: string,
    albumDeleteHash: string,
    observe?: 'response',
    reportProgress?: boolean
  ): Observable<HttpResponse<any>>;
  public albumAddByAlbumDeleteHashPost(
    Deletehashes: string,
    albumDeleteHash: string,
    observe?: 'events',
    reportProgress?: boolean
  ): Observable<HttpEvent<any>>;
  public albumAddByAlbumDeleteHashPost(
    Deletehashes: string,
    albumDeleteHash: string,
    observe: any = 'body',
    reportProgress: boolean = false
  ): Observable<any> {
    if (Deletehashes === null || Deletehashes === undefined) {
      throw new Error(
        'Required parameter Deletehashes was null or undefined when calling albumAddByAlbumDeleteHashPost.'
      );
    }
    if (albumDeleteHash === null || albumDeleteHash === undefined) {
      throw new Error(
        'Required parameter albumDeleteHash was null or undefined when calling albumAddByAlbumDeleteHashPost.'
      );
    }

    let headers = this.defaultHeaders;

    // authentication (auth) required
    if (this.configuration.accessToken) {
      const accessToken =
        typeof this.configuration.accessToken === 'function'
          ? this.configuration.accessToken()
          : this.configuration.accessToken;
      headers = headers.set('Authorization', 'Bearer ' + accessToken);
    }

    // to determine the Accept header
    const httpHeaderAccepts: string[] = ['application/json'];
    const httpHeaderAcceptSelected:
      | string
      | undefined = this.configuration.selectHeaderAccept(httpHeaderAccepts);
    if (httpHeaderAcceptSelected !== undefined) {
      headers = headers.set('Accept', httpHeaderAcceptSelected);
    }

    // to determine the Content-Type header
    const consumes: string[] = ['application/x-www-form-urlencoded'];

    const canConsumeForm = this.canConsumeForm(consumes);

    let formParams: { append(param: string, value: any): void };
    const useForm = false;
    const convertFormParamsToString = false;
    if (useForm) {
      formParams = new FormData();
    } else {
      formParams = new HttpParams({
        encoder: new CustomHttpUrlEncodingCodec()
      });
    }

    if (Deletehashes !== undefined) {
      formParams =
        formParams.append('Deletehashes[]', <any>Deletehashes) || formParams;
    }

    return this.httpClient.post<any>(
      `${this.basePath}/3/album/${encodeURIComponent(
        String(albumDeleteHash)
      )}/add`,
      convertFormParamsToString ? formParams.toString() : formParams,
      {
        withCredentials: this.configuration.withCredentials,
        headers: headers,
        observe: observe,
        reportProgress: reportProgress
      }
    );
  }

  /**
   * Album Deconstion (Authed)
   * Delete an album with a given ID. You are required to be logged in as the user to Delete the album.   #### Response Model: [Basic](https://api.imgur.com/models/basic)
   * @param albumHash
   * @param observe set whether or not to return the data Observable as the body, response or events. defaults to returning the body.
   * @param reportProgress flag to report request and response progress.
   */
  public albumByAlbumHashDelete(
    albumHash: string,
    observe?: 'body',
    reportProgress?: boolean
  ): Observable<any>;
  public albumByAlbumHashDelete(
    albumHash: string,
    observe?: 'response',
    reportProgress?: boolean
  ): Observable<HttpResponse<any>>;
  public albumByAlbumHashDelete(
    albumHash: string,
    observe?: 'events',
    reportProgress?: boolean
  ): Observable<HttpEvent<any>>;
  public albumByAlbumHashDelete(
    albumHash: string,
    observe: any = 'body',
    reportProgress: boolean = false
  ): Observable<any> {
    if (albumHash === null || albumHash === undefined) {
      throw new Error(
        'Required parameter albumHash was null or undefined when calling albumByAlbumHashDelete.'
      );
    }

    let headers = this.defaultHeaders;

    // authentication (auth) required
    if (this.configuration.accessToken) {
      const accessToken =
        typeof this.configuration.accessToken === 'function'
          ? this.configuration.accessToken()
          : this.configuration.accessToken;
      headers = headers.set('Authorization', 'Bearer ' + accessToken);
    }

    // to determine the Accept header
    const httpHeaderAccepts: string[] = ['application/json'];
    const httpHeaderAcceptSelected:
      | string
      | undefined = this.configuration.selectHeaderAccept(httpHeaderAccepts);
    if (httpHeaderAcceptSelected !== undefined) {
      headers = headers.set('Accept', httpHeaderAcceptSelected);
    }

    // to determine the Content-Type header
    const consumes: string[] = ['application/json'];

    return this.httpClient.delete<any>(
      `${this.basePath}/3/album/${encodeURIComponent(String(albumHash))}`,
      {
        withCredentials: this.configuration.withCredentials,
        headers: headers,
        observe: observe,
        reportProgress: reportProgress
      }
    );
  }

  /**
   * Album
   * Get additional information about an album.  #### Response Model: [Album](https://api.imgur.com/models/album)
   * @param albumHash
   * @param observe set whether or not to return the data Observable as the body, response or events. defaults to returning the body.
   * @param reportProgress flag to report request and response progress.
   */
  public albumByAlbumHashGet(
    albumHash: string,
    observe?: 'body',
    reportProgress?: boolean
  ): Observable<any>;
  public albumByAlbumHashGet(
    albumHash: string,
    observe?: 'response',
    reportProgress?: boolean
  ): Observable<HttpResponse<any>>;
  public albumByAlbumHashGet(
    albumHash: string,
    observe?: 'events',
    reportProgress?: boolean
  ): Observable<HttpEvent<any>>;
  public albumByAlbumHashGet(
    albumHash: string,
    observe: any = 'body',
    reportProgress: boolean = false
  ): Observable<any> {
    if (albumHash === null || albumHash === undefined) {
      throw new Error(
        'Required parameter albumHash was null or undefined when calling albumByAlbumHashGet.'
      );
    }

    let headers = this.defaultHeaders;

    // authentication (auth) required
    if (this.configuration.accessToken) {
      const accessToken =
        typeof this.configuration.accessToken === 'function'
          ? this.configuration.accessToken()
          : this.configuration.accessToken;
      headers = headers.set('Authorization', 'Bearer ' + accessToken);
    }

    // to determine the Accept header
    const httpHeaderAccepts: string[] = ['application/json'];
    const httpHeaderAcceptSelected:
      | string
      | undefined = this.configuration.selectHeaderAccept(httpHeaderAccepts);
    if (httpHeaderAcceptSelected !== undefined) {
      headers = headers.set('Accept', httpHeaderAcceptSelected);
    }

    // to determine the Content-Type header
    const consumes: string[] = ['application/json'];

    return this.httpClient.get<any>(
      `${this.basePath}/3/album/${encodeURIComponent(String(albumHash))}`,
      {
        withCredentials: this.configuration.withCredentials,
        headers: headers,
        observe: observe,
        reportProgress: reportProgress
      }
    );
  }

  /**
   * Set Album Images (Authed)
   * Sets the images for an album, removes all other images and only uses the images in this request. You must include either ids[] or Deletehashes[].  #### Response Model: [Basic](https://api.imgur.com/models/basic)  #### Parameters  | Key            | Required | Description                                             | |----------------|----------|---------------------------------------------------------| | ids[]          | optional | The image ids that you want to be added to the album.   | | Deletehashes[] | optional | The image Deletehashes that you want to be added to the album. |
   * @param ids
   * @param albumHash
   * @param observe set whether or not to return the data Observable as the body, response or events. defaults to returning the body.
   * @param reportProgress flag to report request and response progress.
   */
  public albumByAlbumHashPost(
    ids: string,
    albumHash: string,
    observe?: 'body',
    reportProgress?: boolean
  ): Observable<any>;
  public albumByAlbumHashPost(
    ids: string,
    albumHash: string,
    observe?: 'response',
    reportProgress?: boolean
  ): Observable<HttpResponse<any>>;
  public albumByAlbumHashPost(
    ids: string,
    albumHash: string,
    observe?: 'events',
    reportProgress?: boolean
  ): Observable<HttpEvent<any>>;
  public albumByAlbumHashPost(
    ids: string,
    albumHash: string,
    observe: any = 'body',
    reportProgress: boolean = false
  ): Observable<any> {
    if (ids === null || ids === undefined) {
      throw new Error(
        'Required parameter ids was null or undefined when calling albumByAlbumHashPost.'
      );
    }
    if (albumHash === null || albumHash === undefined) {
      throw new Error(
        'Required parameter albumHash was null or undefined when calling albumByAlbumHashPost.'
      );
    }

    let headers = this.defaultHeaders;

    // authentication (auth) required
    if (this.configuration.accessToken) {
      const accessToken =
        typeof this.configuration.accessToken === 'function'
          ? this.configuration.accessToken()
          : this.configuration.accessToken;
      headers = headers.set('Authorization', 'Bearer ' + accessToken);
    }

    // to determine the Accept header
    const httpHeaderAccepts: string[] = ['application/json'];
    const httpHeaderAcceptSelected:
      | string
      | undefined = this.configuration.selectHeaderAccept(httpHeaderAccepts);
    if (httpHeaderAcceptSelected !== undefined) {
      headers = headers.set('Accept', httpHeaderAcceptSelected);
    }

    // to determine the Content-Type header
    const consumes: string[] = ['application/x-www-form-urlencoded'];

    const canConsumeForm = this.canConsumeForm(consumes);

    let formParams: { append(param: string, value: any): void };
    const useForm = false;
    const convertFormParamsToString = false;
    if (useForm) {
      formParams = new FormData();
    } else {
      formParams = new HttpParams({
        encoder: new CustomHttpUrlEncodingCodec()
      });
    }

    if (ids !== undefined) {
      formParams = formParams.append('ids[]', <any>ids) || formParams;
    }

    return this.httpClient.post<any>(
      `${this.basePath}/3/album/${encodeURIComponent(String(albumHash))}`,
      convertFormParamsToString ? formParams.toString() : formParams,
      {
        withCredentials: this.configuration.withCredentials,
        headers: headers,
        observe: observe,
        reportProgress: reportProgress
      }
    );
  }

  /**
   * Update Album (Un-Authed / Authed)
   * Update the information of an album. For anonymous albums, &#x60;albumHash&#x60; should be the Deletehash that is returned at creation.  If uploading anonymous images to an anonymous album please use the optional parameter of &#x60;Deletehashes[]&#x60; rather than &#x60;ids[]&#x60;. Note: including the optional &#x60;Deletehashes[]&#x60; parameter will also work for authenticated user albums. There is no need to duplicate image ids with their corresponding Deletehash.  #### Response Model: [Basic](https://api.imgur.com/models/basic)  #### Parameters  | Key            | Required | Description                                                                                                                          | |----------------|----------|--------------------------------------------------------------------------------------------------------------------------------------| | ids[]          | optional | The image ids that you want to be included in the album.                                                                             | | Deletehashes[] | optional | The Deletehashes of the images that you want to be included in the album.                                                            | | title          | optional | The title of the album                                                                                                               | | description    | optional | The description of the album                                                                                                         | | privacy        | optional | Sets the privacy level of the album. Values are : &#x60;public&#x60; &amp;#124; &#x60;hidden&#x60; &amp;#124; secret. Defaults to user&#39;s privacy settings for logged in users. | | layout         | optional | (_deprecated_) Sets the layout to display the album. Values are : &#x60;blog&#x60; &amp;#124; &#x60;grid&#x60; &amp;#124; &#x60;horizontal&#x60; &amp;#124; &#x60;vertical&#x60;                                               | | cover          | optional | The ID of an image that you want to be the cover of the album                                                                        |
   * @param Deletehashes Optional. The Deletehashes of the images that you want to be included in the album.
   * @param title Optional. The title of the album
   * @param description Optional. The description of the album
   * @param cover Optional. The ID of an image that you want to be the cover of the album
   * @param albumHash
   * @param observe set whether or not to return the data Observable as the body, response or events. defaults to returning the body.
   * @param reportProgress flag to report request and response progress.
   */
  public albumByAlbumHashPut(
    Deletehashes: string,
    title: string,
    description: string,
    cover: string,
    albumHash: string,
    observe?: 'body',
    reportProgress?: boolean
  ): Observable<any>;
  public albumByAlbumHashPut(
    Deletehashes: string,
    title: string,
    description: string,
    cover: string,
    albumHash: string,
    observe?: 'response',
    reportProgress?: boolean
  ): Observable<HttpResponse<any>>;
  public albumByAlbumHashPut(
    Deletehashes: string,
    title: string,
    description: string,
    cover: string,
    albumHash: string,
    observe?: 'events',
    reportProgress?: boolean
  ): Observable<HttpEvent<any>>;
  public albumByAlbumHashPut(
    Deletehashes: string,
    title: string,
    description: string,
    cover: string,
    albumHash: string,
    observe: any = 'body',
    reportProgress: boolean = false
  ): Observable<any> {
    if (Deletehashes === null || Deletehashes === undefined) {
      throw new Error(
        'Required parameter Deletehashes was null or undefined when calling albumByAlbumHashPut.'
      );
    }
    if (title === null || title === undefined) {
      throw new Error(
        'Required parameter title was null or undefined when calling albumByAlbumHashPut.'
      );
    }
    if (description === null || description === undefined) {
      throw new Error(
        'Required parameter description was null or undefined when calling albumByAlbumHashPut.'
      );
    }
    if (cover === null || cover === undefined) {
      throw new Error(
        'Required parameter cover was null or undefined when calling albumByAlbumHashPut.'
      );
    }
    if (albumHash === null || albumHash === undefined) {
      throw new Error(
        'Required parameter albumHash was null or undefined when calling albumByAlbumHashPut.'
      );
    }

    let headers = this.defaultHeaders;

    // authentication (auth) required
    if (this.configuration.accessToken) {
      const accessToken =
        typeof this.configuration.accessToken === 'function'
          ? this.configuration.accessToken()
          : this.configuration.accessToken;
      headers = headers.set('Authorization', 'Bearer ' + accessToken);
    }

    // to determine the Accept header
    const httpHeaderAccepts: string[] = ['application/json'];
    const httpHeaderAcceptSelected:
      | string
      | undefined = this.configuration.selectHeaderAccept(httpHeaderAccepts);
    if (httpHeaderAcceptSelected !== undefined) {
      headers = headers.set('Accept', httpHeaderAcceptSelected);
    }

    // to determine the Content-Type header
    const consumes: string[] = ['application/x-www-form-urlencoded'];

    const canConsumeForm = this.canConsumeForm(consumes);

    let formParams: { append(param: string, value: any): void };
    const useForm = false;
    const convertFormParamsToString = false;
    if (useForm) {
      formParams = new FormData();
    } else {
      formParams = new HttpParams({
        encoder: new CustomHttpUrlEncodingCodec()
      });
    }

    if (Deletehashes !== undefined) {
      formParams =
        formParams.append('Deletehashes', <any>Deletehashes) || formParams;
    }
    if (title !== undefined) {
      formParams = formParams.append('title', <any>title) || formParams;
    }
    if (description !== undefined) {
      formParams =
        formParams.append('description', <any>description) || formParams;
    }
    if (cover !== undefined) {
      formParams = formParams.append('cover', <any>cover) || formParams;
    }

    return this.httpClient.put<any>(
      `${this.basePath}/3/album/${encodeURIComponent(String(albumHash))}`,
      convertFormParamsToString ? formParams.toString() : formParams,
      {
        withCredentials: this.configuration.withCredentials,
        headers: headers,
        observe: observe,
        reportProgress: reportProgress
      }
    );
  }

  /**
   * Favorite Album
   * Favorite an album with a given ID. The user is required to be logged in to favorite the album.  #### Response Model: [Basic](https://api.imgur.com/models/basic)
   * @param albumHash
   * @param observe set whether or not to return the data Observable as the body, response or events. defaults to returning the body.
   * @param reportProgress flag to report request and response progress.
   */
  public albumFavoriteByAlbumHashPost(
    albumHash: string,
    observe?: 'body',
    reportProgress?: boolean
  ): Observable<any>;
  public albumFavoriteByAlbumHashPost(
    albumHash: string,
    observe?: 'response',
    reportProgress?: boolean
  ): Observable<HttpResponse<any>>;
  public albumFavoriteByAlbumHashPost(
    albumHash: string,
    observe?: 'events',
    reportProgress?: boolean
  ): Observable<HttpEvent<any>>;
  public albumFavoriteByAlbumHashPost(
    albumHash: string,
    observe: any = 'body',
    reportProgress: boolean = false
  ): Observable<any> {
    if (albumHash === null || albumHash === undefined) {
      throw new Error(
        'Required parameter albumHash was null or undefined when calling albumFavoriteByAlbumHashPost.'
      );
    }

    let headers = this.defaultHeaders;

    // authentication (auth) required
    if (this.configuration.accessToken) {
      const accessToken =
        typeof this.configuration.accessToken === 'function'
          ? this.configuration.accessToken()
          : this.configuration.accessToken;
      headers = headers.set('Authorization', 'Bearer ' + accessToken);
    }

    // to determine the Accept header
    const httpHeaderAccepts: string[] = ['application/json'];
    const httpHeaderAcceptSelected:
      | string
      | undefined = this.configuration.selectHeaderAccept(httpHeaderAccepts);
    if (httpHeaderAcceptSelected !== undefined) {
      headers = headers.set('Accept', httpHeaderAcceptSelected);
    }

    // to determine the Content-Type header
    const consumes: string[] = ['application/json'];

    return this.httpClient.post<any>(
      `${this.basePath}/3/album/${encodeURIComponent(
        String(albumHash)
      )}/favorite`,
      null,
      {
        withCredentials: this.configuration.withCredentials,
        headers: headers,
        observe: observe,
        reportProgress: reportProgress
      }
    );
  }

  /**
   * Album Image
   * Get information about an image in an album, any additional actions found in [Image Endpoint](https://api.imgur.com/endpoints/image/) will also work.  #### Response Model: [Image](https://api.imgur.com/models/image)
   * @param albumHash
   * @param imageHash
   * @param observe set whether or not to return the data Observable as the body, response or events. defaults to returning the body.
   * @param reportProgress flag to report request and response progress.
   */
  public albumImageByAlbumHashAndImageHashGet(
    albumHash: string,
    imageHash: string,
    observe?: 'body',
    reportProgress?: boolean
  ): Observable<any>;
  public albumImageByAlbumHashAndImageHashGet(
    albumHash: string,
    imageHash: string,
    observe?: 'response',
    reportProgress?: boolean
  ): Observable<HttpResponse<any>>;
  public albumImageByAlbumHashAndImageHashGet(
    albumHash: string,
    imageHash: string,
    observe?: 'events',
    reportProgress?: boolean
  ): Observable<HttpEvent<any>>;
  public albumImageByAlbumHashAndImageHashGet(
    albumHash: string,
    imageHash: string,
    observe: any = 'body',
    reportProgress: boolean = false
  ): Observable<any> {
    if (albumHash === null || albumHash === undefined) {
      throw new Error(
        'Required parameter albumHash was null or undefined when calling albumImageByAlbumHashAndImageHashGet.'
      );
    }
    if (imageHash === null || imageHash === undefined) {
      throw new Error(
        'Required parameter imageHash was null or undefined when calling albumImageByAlbumHashAndImageHashGet.'
      );
    }

    let headers = this.defaultHeaders;

    // authentication (auth) required
    if (this.configuration.accessToken) {
      const accessToken =
        typeof this.configuration.accessToken === 'function'
          ? this.configuration.accessToken()
          : this.configuration.accessToken;
      headers = headers.set('Authorization', 'Bearer ' + accessToken);
    }

    // to determine the Accept header
    const httpHeaderAccepts: string[] = ['application/json'];
    const httpHeaderAcceptSelected:
      | string
      | undefined = this.configuration.selectHeaderAccept(httpHeaderAccepts);
    if (httpHeaderAcceptSelected !== undefined) {
      headers = headers.set('Accept', httpHeaderAcceptSelected);
    }

    // to determine the Content-Type header
    const consumes: string[] = ['application/json'];

    return this.httpClient.get<any>(
      `${this.basePath}/3/album/${encodeURIComponent(
        String(albumHash)
      )}/image/${encodeURIComponent(String(imageHash))}`,
      {
        withCredentials: this.configuration.withCredentials,
        headers: headers,
        observe: observe,
        reportProgress: reportProgress
      }
    );
  }

  /**
   * Album Images
   * Return all of the images in the album.  #### Response Model: [Image](https://api.imgur.com/models/image)
   * @param albumHash
   * @param observe set whether or not to return the data Observable as the body, response or events. defaults to returning the body.
   * @param reportProgress flag to report request and response progress.
   */
  public albumImagesByAlbumHashGet(
    albumHash: string,
    observe?: 'body',
    reportProgress?: boolean
  ): Observable<any>;
  public albumImagesByAlbumHashGet(
    albumHash: string,
    observe?: 'response',
    reportProgress?: boolean
  ): Observable<HttpResponse<any>>;
  public albumImagesByAlbumHashGet(
    albumHash: string,
    observe?: 'events',
    reportProgress?: boolean
  ): Observable<HttpEvent<any>>;
  public albumImagesByAlbumHashGet(
    albumHash: string,
    observe: any = 'body',
    reportProgress: boolean = false
  ): Observable<any> {
    if (albumHash === null || albumHash === undefined) {
      throw new Error(
        'Required parameter albumHash was null or undefined when calling albumImagesByAlbumHashGet.'
      );
    }

    let headers = this.defaultHeaders;

    // authentication (auth) required
    if (this.configuration.accessToken) {
      const accessToken =
        typeof this.configuration.accessToken === 'function'
          ? this.configuration.accessToken()
          : this.configuration.accessToken;
      headers = headers.set('Authorization', 'Bearer ' + accessToken);
    }

    // to determine the Accept header
    const httpHeaderAccepts: string[] = ['application/json'];
    const httpHeaderAcceptSelected:
      | string
      | undefined = this.configuration.selectHeaderAccept(httpHeaderAccepts);
    if (httpHeaderAcceptSelected !== undefined) {
      headers = headers.set('Accept', httpHeaderAcceptSelected);
    }

    // to determine the Content-Type header
    const consumes: string[] = ['application/json'];

    return this.httpClient.get<any>(
      `${this.basePath}/3/album/${encodeURIComponent(
        String(albumHash)
      )}/images`,
      {
        withCredentials: this.configuration.withCredentials,
        headers: headers,
        observe: observe,
        reportProgress: reportProgress
      }
    );
  }

  /**
   * Album Creation (Un-Authed / Authed)
   * Create a new album. Optional parameter of &#x60;ids[]&#x60; is an array of image ids to add to the album. If uploading anonymous images to an anonymous album please use the optional parameter of &#x60;Deletehashes[]&#x60; rather than &#x60;ids[]&#x60;. Note: including the optional &#x60;Deletehashes[]&#x60; parameter will also work for authenticated user albums. There is no need to duplicate image ids with their corresponding Deletehash.  This method is available without authenticating an account, and may be used merely by sending \&quot;Authorization: Client-ID {client_id}\&quot; in the request headers. Doing so will create an anonymous album which is not tied to an account.  #### Response Model: [Basic](https://api.imgur.com/models/basic)  #### Parameters  | Key            | Required | Description                                                                                                                          | |----------------|----------|--------------------------------------------------------------------------------------------------------------------------------------| | ids[]          | optional | The image ids that you want to be included in the album.                                                                             | | Deletehashes[] | optional | The Deletehashes of the images that you want to be included in the album.                                                            | | title          | optional | The title of the album                                                                                                               | | description    | optional | The description of the album                                                                                                         | | privacy        | optional | Sets the privacy level of the album. Values are : &#x60;public&#x60; &amp;#124; &#x60;hidden&#x60; &amp;#124; secret. Defaults to user&#39;s privacy settings for logged in users. | | layout         | optional | (_deprecated_) Sets the layout to display the album. Values are : &#x60;blog&#x60; &amp;#124; &#x60;grid&#x60; &amp;#124; &#x60;horizontal&#x60; &amp;#124; &#x60;vertical&#x60;                                               | | cover          | optional | The ID of an image that you want to be the cover of the album                                                                        |
   * @param ids The image ids that you want to be included in the album.
   * @param title The title of the album
   * @param description The description of the album
   * @param cover The ID of an image that you want to be the cover of the album
   * @param observe set whether or not to return the data Observable as the body, response or events. defaults to returning the body.
   * @param reportProgress flag to report request and response progress.
   */
  public albumPost(
    ids: string,
    title: string,
    description: string,
    cover: string,
    observe?: 'body',
    reportProgress?: boolean
  ): Observable<any>;
  public albumPost(
    ids: string,
    title: string,
    description: string,
    cover: string,
    observe?: 'response',
    reportProgress?: boolean
  ): Observable<HttpResponse<any>>;
  public albumPost(
    ids: string,
    title: string,
    description: string,
    cover: string,
    observe?: 'events',
    reportProgress?: boolean
  ): Observable<HttpEvent<any>>;
  public albumPost(
    ids: string,
    title: string,
    description: string,
    cover: string,
    observe: any = 'body',
    reportProgress: boolean = false
  ): Observable<any> {
    if (ids === null || ids === undefined) {
      throw new Error(
        'Required parameter ids was null or undefined when calling albumPost.'
      );
    }
    if (title === null || title === undefined) {
      throw new Error(
        'Required parameter title was null or undefined when calling albumPost.'
      );
    }
    if (description === null || description === undefined) {
      throw new Error(
        'Required parameter description was null or undefined when calling albumPost.'
      );
    }
    if (cover === null || cover === undefined) {
      throw new Error(
        'Required parameter cover was null or undefined when calling albumPost.'
      );
    }

    let headers = this.defaultHeaders;

    // authentication (auth) required
    if (this.configuration.accessToken) {
      const accessToken =
        typeof this.configuration.accessToken === 'function'
          ? this.configuration.accessToken()
          : this.configuration.accessToken;
      headers = headers.set('Authorization', 'Bearer ' + accessToken);
    }

    // to determine the Accept header
    const httpHeaderAccepts: string[] = ['application/json'];
    const httpHeaderAcceptSelected:
      | string
      | undefined = this.configuration.selectHeaderAccept(httpHeaderAccepts);
    if (httpHeaderAcceptSelected !== undefined) {
      headers = headers.set('Accept', httpHeaderAcceptSelected);
    }

    // to determine the Content-Type header
    const consumes: string[] = ['application/x-www-form-urlencoded'];

    const canConsumeForm = this.canConsumeForm(consumes);

    let formParams: { append(param: string, value: any): void };
    const useForm = false;
    const convertFormParamsToString = false;
    if (useForm) {
      formParams = new FormData();
    } else {
      formParams = new HttpParams({
        encoder: new CustomHttpUrlEncodingCodec()
      });
    }

    if (ids !== undefined) {
      formParams = formParams.append('ids[]', <any>ids) || formParams;
    }
    if (title !== undefined) {
      formParams = formParams.append('title', <any>title) || formParams;
    }
    if (description !== undefined) {
      formParams =
        formParams.append('description', <any>description) || formParams;
    }
    if (cover !== undefined) {
      formParams = formParams.append('cover', <any>cover) || formParams;
    }

    return this.httpClient.post<any>(
      `${this.basePath}/3/album`,
      convertFormParamsToString ? formParams.toString() : formParams,
      {
        withCredentials: this.configuration.withCredentials,
        headers: headers,
        observe: observe,
        reportProgress: reportProgress
      }
    );
  }

  /**
   * Remove Images from an Album (Un-Authed)
   * Takes parameter, &#x60;ids[]&#x60;, as an array of ids and removes from the album.  #### Response Model: [Basic](https://api.imgur.com/models/basic)  #### Parameters  | Key   | Required | Description                                           | |-------|----------|-------------------------------------------------------| | ids[] | required | The image ids that you want to be removed from the album. |
   * @param ids
   * @param albumDeleteHash
   * @param observe set whether or not to return the data Observable as the body, response or events. defaults to returning the body.
   * @param reportProgress flag to report request and response progress.
   */
  public albumRemoveImagesByAlbumDeleteHashPost(
    ids: string,
    albumDeleteHash: string,
    observe?: 'body',
    reportProgress?: boolean
  ): Observable<any>;
  public albumRemoveImagesByAlbumDeleteHashPost(
    ids: string,
    albumDeleteHash: string,
    observe?: 'response',
    reportProgress?: boolean
  ): Observable<HttpResponse<any>>;
  public albumRemoveImagesByAlbumDeleteHashPost(
    ids: string,
    albumDeleteHash: string,
    observe?: 'events',
    reportProgress?: boolean
  ): Observable<HttpEvent<any>>;
  public albumRemoveImagesByAlbumDeleteHashPost(
    ids: string,
    albumDeleteHash: string,
    observe: any = 'body',
    reportProgress: boolean = false
  ): Observable<any> {
    if (ids === null || ids === undefined) {
      throw new Error(
        'Required parameter ids was null or undefined when calling albumRemoveImagesByAlbumDeleteHashPost.'
      );
    }
    if (albumDeleteHash === null || albumDeleteHash === undefined) {
      throw new Error(
        'Required parameter albumDeleteHash was null or undefined when calling albumRemoveImagesByAlbumDeleteHashPost.'
      );
    }

    let headers = this.defaultHeaders;

    // authentication (auth) required
    if (this.configuration.accessToken) {
      const accessToken =
        typeof this.configuration.accessToken === 'function'
          ? this.configuration.accessToken()
          : this.configuration.accessToken;
      headers = headers.set('Authorization', 'Bearer ' + accessToken);
    }

    // to determine the Accept header
    const httpHeaderAccepts: string[] = ['application/json'];
    const httpHeaderAcceptSelected:
      | string
      | undefined = this.configuration.selectHeaderAccept(httpHeaderAccepts);
    if (httpHeaderAcceptSelected !== undefined) {
      headers = headers.set('Accept', httpHeaderAcceptSelected);
    }

    // to determine the Content-Type header
    const consumes: string[] = ['application/x-www-form-urlencoded'];

    const canConsumeForm = this.canConsumeForm(consumes);

    let formParams: { append(param: string, value: any): void };
    const useForm = false;
    const convertFormParamsToString = false;
    if (useForm) {
      formParams = new FormData();
    } else {
      formParams = new HttpParams({
        encoder: new CustomHttpUrlEncodingCodec()
      });
    }

    if (ids !== undefined) {
      formParams = formParams.append('ids[]', <any>ids) || formParams;
    }

    return this.httpClient.post<any>(
      `${this.basePath}/3/album/${encodeURIComponent(
        String(albumDeleteHash)
      )}/remove_images`,
      convertFormParamsToString ? formParams.toString() : formParams,
      {
        withCredentials: this.configuration.withCredentials,
        headers: headers,
        observe: observe,
        reportProgress: reportProgress
      }
    );
  }
}