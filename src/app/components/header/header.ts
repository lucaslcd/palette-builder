import { Component } from '@angular/core';
import { MatButton } from '@angular/material/button';
import { MatToolbar } from "@angular/material/toolbar";

@Component({
  selector: 'pbu-header',
  imports: [MatToolbar, MatButton],
  templateUrl: './header.html',
  styleUrl: './header.css',
})
export class Header {}
