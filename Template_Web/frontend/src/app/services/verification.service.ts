import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { tap } from 'rxjs/operators';
import { catchError } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class VerificationService {
  private apiUrl = 'http://localhost:8080/api'; // Use your chosen port

  constructor(private http: HttpClient) {}
// Add console logging
verifyIdDocument(document: File): Observable<any> {
  const formData = new FormData();
  formData.append('document', document);
  
  console.log('Sending document to API:', document.name, document.size, document.type);
  
  return this.http.post(`${this.apiUrl}/verify/id`, formData).pipe(
    tap(response => console.log('API response:', response)),
    catchError(error => {
      console.error('API error:', error);
      return throwError(() => error);
    })
  );
}
  verifyRoleDocument(document: File, role: string): Observable<any> {
    const formData = new FormData();
    formData.append('document', document);
    formData.append('role', role);
    
    return this.http.post(`${this.apiUrl}/verify/document/${role.toLowerCase().replace(' ', '_')}`, formData);
  }

  verifyFaceMatch(idImage: File, selfieBase64: string): Observable<any> {
    const formData = new FormData();
    formData.append('id_image', idImage);
    formData.append('selfie_base64', selfieBase64);
    
    return this.http.post(`${this.apiUrl}/verify/face`, formData);
  }
}