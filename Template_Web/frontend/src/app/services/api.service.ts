import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { Recommendation } from '../models/recommendation.model';

@Injectable({
  providedIn: 'root'
})
export class ApiService {
  private apiUrl = 'http://localhost:5000/api';
  
  constructor(private http: HttpClient) {}
  
  getRecommendations(userId: string): Observable<Recommendation[]> {
    return this.http.get<Recommendation[]>(`${this.apiUrl}/recommendations?userId=${userId}`)
      .pipe(
        catchError(error => {
          console.error('Error fetching recommendations:', error);
          return throwError(() => error);
        })
      );
  }
  
  saveConnection(userId: string, entityId: string): Observable<any> {
    return this.http.post(`${this.apiUrl}/connections`, { userId, entityId })
      .pipe(
        catchError(error => {
          console.error('Error saving connection:', error);
          return throwError(() => error);
        })
      );
  }
  
  removeConnection(userId: string, entityId: string): Observable<any> {
    return this.http.delete(`${this.apiUrl}/connections/${userId}/${entityId}`)
      .pipe(
        catchError(error => {
          console.error('Error removing connection:', error);
          return throwError(() => error);
        })
      );
  }
  
  saveUserPreferences(userId: string, preferences: any): Observable<any> {
    return this.http.put(`${this.apiUrl}/users/${userId}/preferences`, { filterPreferences: preferences })
      .pipe(
        catchError(error => {
          console.error('Error saving preferences:', error);
          return throwError(() => error);
        })
      );
  }
  
  getUserPreferences(userId: string): Observable<any> {
    return this.http.get(`${this.apiUrl}/users/${userId}/preferences`)
      .pipe(
        catchError(error => {
          console.error('Error getting preferences:', error);
          return throwError(() => error);
        })
      );
  }
}