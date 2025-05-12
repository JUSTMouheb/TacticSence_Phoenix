import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class LoginService {
  private showLoginModalSource = new Subject<void>();
  showLoginModal$ = this.showLoginModalSource.asObservable();
  
  openLoginModal() {
    this.showLoginModalSource.next();
  }
}